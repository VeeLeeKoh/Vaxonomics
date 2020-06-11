import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2020, 4, 29)
}

staging_dataset = 'faers_workflow_staging'
modeled_dataset = 'faers_workflow_modeled'

bq_query_start = 'bq query --use_legacy_sql=false '

# Fix a record with no manufacturers in Demographic table to prevent "None" manufacturer record

demo_manu_fix_sql = 'UPDATE ' + staging_dataset + '''.Demographic
                     SET mfr_sndr = "FDA-CTU"
                     WHERE mfr_sndr is null'''

# Fix a record with age less than 0 in Demographic table

demo_age_fix_sql = 'UPDATE ' + staging_dataset + '''.Demographic
                    SET age = 10
                    WHERE age < 0'''

# Fix age record errors in Demographic table

demo_age_error_sql = 'UPDATE ' + staging_dataset + '''.Demographic
                      SET age_cod = "YR"
                      WHERE age_cod = "HR" and age_grp="A"''' 

demo_age_error_sql2 = 'UPDATE ' + staging_dataset + '''.Demographic
                       SET age_cod = "YR"
                       WHERE age_cod = "WK" and age_grp="A"'''

demo_age_error_sql3 = 'UPDATE ' + staging_dataset + '''.Demographic
                       SET age_grp = "N"
                       WHERE age_cod = "DY" and age_grp="A"'''

# Fix date record errors in Demographic table

demo_date_fix_sql = 'UPDATE ' + staging_dataset + '''.Demographic
                     SET event_dt_num = "2018-09-26"
                     WHERE event_dt_num = "2108-09-26"'''

demo_date_fix_sql2 = 'UPDATE ' + staging_dataset + '''.Demographic
                      SET event_dt_num = "2018-07-02"
                      WHERE event_dt_num = "1118-07-02"'''

demo_date_fix_sql3 = 'UPDATE ' + staging_dataset + '''.Demographic
                      SET event_dt_num = "2018-03-02"
                      WHERE event_dt_num = "1018-03-02"'''

demo_date_fix_sql4 = 'UPDATE ' + staging_dataset + '''.Demographic
                      SET event_dt_num = "2018-01-21"
                      WHERE event_dt_num = "1018-01-21"'''

demo_date_fix_sql5 = 'UPDATE ' + staging_dataset + '''.Demographic
                      SET event_dt_num = "2018-08-28"
                      WHERE event_dt_num = "2019-08-28"'''

demo_date_fix_sql6 = 'UPDATE ' + staging_dataset + '''.Demographic
                      SET event_dt_num = "2018-07-29"
                      WHERE event_dt_num = "2019-07-29"'''

# Replace 'None' record of PROD_AI (Product Active_Ingredient) in Drug table with 'UNKNOWN' to avoid null records of active ingredients

drug_ing_fix_sql = 'UPDATE ' + staging_dataset + '''.Drug
                    SET PROD_AI = "UNKNOWN"
                    WHERE PROD_AI is null'''

# MODELED TABLE SQL BEGINS HERE
# Split Demographic table into separate entities: Patient table, Manufacturer table, Adverse_Event table and Case table

demo_pat_split_sql = 'create or replace table ' + modeled_dataset + '''.Patient as
                      select ROW_NUMBER() OVER(ORDER BY PRIMARYID) as PATIENT_ID, PRIMARYID as CASE_ID, SEX, AGE, AGE_COD as AGE_UNIT, AGE_GRP as                             AGE_GROUP, WT as WEIGHT, WT_COD as WEIGHT_UNIT 
                      from ''' + staging_dataset + '.Demographic'

demo_manu_split_sql = 'create or replace table ' + modeled_dataset + '''.Manufacturer as
                       select ROW_NUMBER() OVER(ORDER BY MFR_SNDR) as MANU_ID, MFR_SNDR as DRUG_MANU 
                       from ''' + staging_dataset + '''.Demographic
                       group by MFR_SNDR'''

demo_ae_split_sql = 'create or replace table ' + modeled_dataset + '''.Adverse_Event as
                     select ROW_NUMBER() OVER(ORDER BY d.PRIMARYID) as EVENT_ID, d.PRIMARYID as CASE_ID, EVENT_DT_NUM as EVENT_DATE, OCCR_COUNTRY as                        COUNTRY, OUTC_COD as OUTCOME
                     from ''' + staging_dataset + '.Demographic d left join ' + staging_dataset + '.Outcome o on d.primaryid = o.primaryid'

demo_case_split_sql = 'create or replace table ' + modeled_dataset + '''.Case as
                       select PRIMARYID as CASE_ID, I_F_CODE as STATUS, REPT_COD as TYPE, REPT_DT_NUM as CASE_DATE, FDA_DT_NUM as FDA_DATE, MFR_DT_NUM                        as MANU_DATE, MANU_ID, TO_MFR as MANU_NOTIFD, OCCP_COD as REPORTER_OCCP, REPORTER_COUNTRY 
                       from ''' + staging_dataset + '.Demographic d join ' + modeled_dataset + '.Manufacturer m on d.MFR_SNDR = m.DRUG_MANU '

# Split Drug table into separate entities: Drug table (unique drug records), Active_Ingredient table and Administration (drug dispensing) table

drug_uni_split_sql = 'create or replace table ' + modeled_dataset + '''.Drug as 
                      select ROW_NUMBER() OVER() as DRUG_ID, DRUGNAME as DRUG_NAME
                      from ''' + staging_dataset + '''.Drug
                      group by DRUGNAME'''

drug_ai_split_sql = 'create or replace table ' + modeled_dataset + '''.Active_Ingredient as 
                     select ROW_NUMBER() OVER(ORDER BY PROD_AI) as INGREDIENT_ID, PROD_AI as ACTIVE_INGREDIENT
                     from ''' + staging_dataset + '''.Drug
                     group by PROD_AI'''

drug_admin_split_sql = 'create or replace table ' + modeled_dataset + '''.Administration as 
                        select ROW_NUMBER() OVER(ORDER BY PRIMARYID) as ADMIN_ID, PRIMARYID as CASE_ID, DRUG_ID, INGREDIENT_ID, DRUG_SEQ, ROLE_COD as                           DRUG_ROLE, LOT_NUM as DRUG_LOT, ROUTE as DRUG_ROUTE, DOSE_FORM, DOSE_FREQ, DOSE_AMT, DOSE_UNIT, CUM_DOSE_CHR as CUM_DOSE,                               CUM_DOSE_UNIT, DECHAL, RECHAL  
                        from ''' + staging_dataset + '.Drug d left join ' + modeled_dataset + '''.Drug d2 on d.DRUGNAME = d2.DRUG_NAME
                            left join ''' + modeled_dataset + '.Active_Ingredient ai on d.PROD_AI = ai.ACTIVE_INGREDIENT'

# Generate Primary Key for Reaction table and join with Case table for Foreign Key

reac_pk_sql = 'create or replace table ' + modeled_dataset + '''.Reaction as 
               select ROW_NUMBER() OVER(ORDER BY CASE_ID) as REACTION_ID, CASE_ID, PT as REACTION, DRUG_REC_ACT as RECUR_REACTION
               from ''' + staging_dataset + '.Reaction r left join ' + modeled_dataset + '.Case c on r.PRIMARYID = c.CASE_ID'

# Create Diagnosis table from Indication table and join with Administration table for Foreign Key

create_diag_sql = 'create or replace table ' + modeled_dataset + '''.Diagnosis as 
            select ROW_NUMBER() OVER(ORDER BY ADMIN_ID) as DIAGNOSIS_ID, ADMIN_ID, INDI_PT as DIAGNOSIS
            from ''' + staging_dataset + '.Indication i left join ' + modeled_dataset + '''.Administration a on i.PRIMARYID = a.CASE_ID and                         i.INDI_DRUG_SEQ = a.DRUG_SEQ'''

# TRANSFORMATION SQL BEGINS HERE

#Standardize age in unit of years on FAERS Patient table using SQL

# Step 1: Create intermediate table to convert age in months to years (1 year has 12 months)

inter_table_sql1 = 'create or replace table ' + modeled_dataset + '''.Patient_SQL_1 as
                 select *, CAST(FLOOR(AGE/12) as INT64) as AGE_YRS 
                 from ''' + modeled_dataset + '''.Patient
                 where AGE_UNIT = "MON" and AGE is not null'''

# Step 2: Create intermediate table to convert age in weeks to years (1 year has approximately 52.14 weeks)

inter_table_sql2 = 'create or replace table ' + modeled_dataset + '''.Patient_SQL_2 as
                 select *, CAST(FLOOR(AGE/52.14) as INT64) as AGE_YRS 
                 from ''' + modeled_dataset + '''.Patient
                 where AGE_UNIT = "WK" and AGE is not null'''

# Step 3: Create intermediate table to convert age in days to years (1 year has 365 days)

inter_table_sql3 = 'create or replace table ' + modeled_dataset + '''.Patient_SQL_3 as
                 select *, CAST(FLOOR(AGE/365) as INT64) as AGE_YRS 
                 from ''' + modeled_dataset + '''.Patient
                 where AGE_UNIT = "DY" and AGE is not null'''

# Step 4: Create intermediate table to convert age in hours to years (1 year has 365 x 24 hours)

inter_table_sql4 = 'create or replace table ' + modeled_dataset + '''.Patient_SQL_4 as
                 select *, CAST(FLOOR(AGE/24/365) as INT64) as AGE_YRS 
                 from ''' + modeled_dataset + '''.Patient
                 where AGE_UNIT = "HR" and AGE is not null'''

# Step 5: Create intermediate table to convert age in decades to years (1 decade has 10 years)

inter_table_sql5 = 'create or replace table ' + modeled_dataset + '''.Patient_SQL_5 as
                 select *, CAST(FLOOR(AGE*10) as INT64) as AGE_YRS 
                 from ''' + modeled_dataset + '''.Patient
                 where AGE_UNIT = "DEC" and AGE is not null'''

# Step 6: Create final table based on Patient_SQL_1 + Patient_SQL_2 + Patient_SQL_3 + Patient_SQL_4 + Patient_SQL_5 + age in years & null ages from original Patient table

final_transform_sql = 'create or replace table ' + modeled_dataset + '''.Patient_SQL_Final as
                       (select PATIENT_ID, CASE_ID, SEX, AGE_YRS, AGE_GROUP, WEIGHT, WEIGHT_UNIT
                        from ''' + modeled_dataset + '''.Patient_SQL_1
                       union all
                        select PATIENT_ID, CASE_ID, SEX, AGE_YRS, AGE_GROUP, WEIGHT, WEIGHT_UNIT
                        from ''' + modeled_dataset + '''.Patient_SQL_2
                       union all
                        select PATIENT_ID, CASE_ID, SEX, AGE_YRS, AGE_GROUP, WEIGHT, WEIGHT_UNIT
                        from ''' + modeled_dataset + '''.Patient_SQL_3
                       union all
                        select PATIENT_ID, CASE_ID, SEX, AGE_YRS, AGE_GROUP, WEIGHT, WEIGHT_UNIT
                        from ''' + modeled_dataset + '''.Patient_SQL_4
                       union all
                        select PATIENT_ID, CASE_ID, SEX, AGE_YRS, AGE_GROUP, WEIGHT, WEIGHT_UNIT
                        from ''' + modeled_dataset + '''.Patient_SQL_5
                       union all
                        select PATIENT_ID, CASE_ID, SEX, AGE as AGE_YRS, AGE_GROUP, WEIGHT, WEIGHT_UNIT
                        from ''' + modeled_dataset + '''.Patient
                        where AGE is null or AGE_UNIT = "YR"
                       )'''

with models.DAG(
        'faers_workflow',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    create_staging = BashOperator(
            task_id='create_staging_dataset',
            bash_command='bq --location=US mk --dataset ' + staging_dataset)
    
    create_modeled = BashOperator(
            task_id='create_modeled_dataset',
            bash_command='bq --location=US mk --dataset ' + modeled_dataset)
    
    load_demo = BashOperator(
            task_id='load_demo',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.Demographic \
                         "gs://cs327e_project_data/drug_data/demo2018q4.csv"\
                         primaryid:INT64,caseid:INT64,caseversion:INT64,i_f_code:STRING,i_f_code_num:INT64,event_dt:INT64,\
event_dt_num:DATE,mfr_dt:INT64,mfr_dt_num:DATE,init_fda_dt:INT64,init_fda_dt_num:DATE,fda_dt:INT64,fda_dt_num:DATE,\
rept_cod:STRING,rept_cod_num:INT64,auth_num:STRING,mfr_num:STRING,mfr_sndr:STRING,lit_ref:STRING,age:INT64,\
age_cod:STRING,age_grp:STRING,age_grp_num:STRING,sex:STRING,e_sub:STRING,wt:FLOAT64,wt_cod:STRING,rept_dt:INT64,\
rept_dt_num:DATE,to_mfr:STRING,occp_cod:STRING,reporter_country:STRING,occr_country:STRING,occp_cod_num:INT64',
            trigger_rule='one_success')
    
    load_drug = BashOperator(
            task_id='load_drug',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.Drug \
                         "gs://cs327e_project_data/drug_data/drug2018q4.csv"',
            trigger_rule='one_success')
    
    load_reaction = BashOperator(
            task_id='load_reaction',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.Reaction \
                         "gs://cs327e_project_data/drug_data/reac2018q4.csv"',
            trigger_rule='one_success')
    
    load_outcome = BashOperator(
            task_id='load_outcome',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.Outcome \
                         "gs://cs327e_project_data/drug_data/outc2018q4.csv"', 
            trigger_rule='one_success')
    
    load_indication = BashOperator(
            task_id='load_indication',
            bash_command='bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV ' + staging_dataset + '.Indication \
                         "gs://cs327e_project_data/drug_data/indi2018q4.csv"', 
            trigger_rule='one_success')
   
    branch = DummyOperator(
            task_id='branch',
            trigger_rule='all_done')
    
    branch2 = DummyOperator(
            task_id='branch2',
            trigger_rule='all_done')
    
    join = DummyOperator(
            task_id='join',
            trigger_rule='all_done')
    
    join2 = DummyOperator(
            task_id='join2',
            trigger_rule='all_done')
    
    join3 = DummyOperator(
            task_id='join3',
            trigger_rule='all_done')
    
    join4 = DummyOperator(
            task_id='join4',
            trigger_rule='all_done')

    demo_fix_manu = BashOperator(
            task_id='demo_fix_manu',
            bash_command=bq_query_start + "'" + demo_manu_fix_sql + "'", 
            trigger_rule='one_success')
    
    demo_fix_age = BashOperator(
            task_id='demo_fix_age',
            bash_command=bq_query_start + "'" + demo_age_fix_sql + "'", 
            trigger_rule='one_success')
    
    demo_age_error = BashOperator(
            task_id='demo_age_error',
            bash_command=bq_query_start + "'" + demo_age_error_sql + "'", 
            trigger_rule='one_success')

    demo_age_error2 = BashOperator(
            task_id='demo_age_error2',
            bash_command=bq_query_start + "'" + demo_age_error_sql2 + "'", 
            trigger_rule='one_success')
    
    demo_age_error3 = BashOperator(
            task_id='demo_age_error3',
            bash_command=bq_query_start + "'" + demo_age_error_sql3 + "'", 
            trigger_rule='one_success')
    
    demo_date_fix = BashOperator(
            task_id='demo_date_fix',
            bash_command=bq_query_start + "'" + demo_date_fix_sql + "'", 
            trigger_rule='one_success')
    
    demo_date_fix2 = BashOperator(
            task_id='demo_date_fix2',
            bash_command=bq_query_start + "'" + demo_date_fix_sql2 + "'", 
            trigger_rule='one_success')
    
    demo_date_fix3 = BashOperator(
            task_id='demo_date_fix3',
            bash_command=bq_query_start + "'" + demo_date_fix_sql3 + "'", 
            trigger_rule='one_success')
    
    demo_date_fix4 = BashOperator(
            task_id='demo_date_fix4',
            bash_command=bq_query_start + "'" + demo_date_fix_sql4 + "'", 
            trigger_rule='one_success')
    
    demo_date_fix5 = BashOperator(
            task_id='demo_date_fix5',
            bash_command=bq_query_start + "'" + demo_date_fix_sql5 + "'", 
            trigger_rule='one_success')
    
    demo_date_fix6 = BashOperator(
            task_id='demo_date_fix6',
            bash_command=bq_query_start + "'" + demo_date_fix_sql6 + "'", 
            trigger_rule='one_success')
    
    drug_ing_fix = BashOperator(
            task_id='drug_ing_fix',
            bash_command=bq_query_start + "'" + drug_ing_fix_sql + "'", 
            trigger_rule='one_success')
    
    demo_pat_split = BashOperator(
            task_id='demo_pat_split',
            bash_command=bq_query_start + "'" + demo_pat_split_sql + "'", 
            trigger_rule='one_success')
    
    demo_manu_split = BashOperator(
            task_id='demo_manu_split',
            bash_command=bq_query_start + "'" + demo_manu_split_sql + "'", 
            trigger_rule='one_success')
    
    demo_ae_split = BashOperator(
            task_id='demo_ae_split',
            bash_command=bq_query_start + "'" + demo_ae_split_sql + "'", 
            trigger_rule='one_success')
    
    demo_case_split = BashOperator(
            task_id='demo_case_split',
            bash_command=bq_query_start + "'" + demo_case_split_sql + "'", 
            trigger_rule='one_success')
    
    drug_uni_split = BashOperator(
            task_id='drug_uni_split',
            bash_command=bq_query_start + "'" + drug_uni_split_sql + "'", 
            trigger_rule='one_success')
    
    drug_ai_split = BashOperator(
            task_id='drug_ai_split',
            bash_command=bq_query_start + "'" + drug_ai_split_sql + "'", 
            trigger_rule='one_success')
    
    drug_admin_split = BashOperator(
            task_id='drug_admin_split',
            bash_command=bq_query_start + "'" + drug_admin_split_sql + "'", 
            trigger_rule='one_success')
    
    reac_pk = BashOperator(
            task_id='reac_pk',
            bash_command=bq_query_start + "'" + reac_pk_sql + "'", 
            trigger_rule='one_success')
    
    create_diag = BashOperator(
            task_id='create_diag',
            bash_command=bq_query_start + "'" + create_diag_sql + "'", 
            trigger_rule='one_success')
    
    inter_table_1 = BashOperator(
            task_id='inter_table_1',
            bash_command=bq_query_start + "'" + inter_table_sql1 + "'", 
            trigger_rule='one_success')

    inter_table_2 = BashOperator(
            task_id='inter_table_2',
            bash_command=bq_query_start + "'" + inter_table_sql2 + "'", 
            trigger_rule='one_success')
    
    inter_table_3 = BashOperator(
            task_id='inter_table_3',
            bash_command=bq_query_start + "'" + inter_table_sql3 + "'", 
            trigger_rule='one_success')
    
    inter_table_4 = BashOperator(
            task_id='inter_table_4',
            bash_command=bq_query_start + "'" + inter_table_sql4 + "'", 
            trigger_rule='one_success')
    
    inter_table_5 = BashOperator(
            task_id='inter_table_5',
            bash_command=bq_query_start + "'" + inter_table_sql5 + "'", 
            trigger_rule='one_success')
    
    final_transform = BashOperator(
            task_id='final_transform',
            bash_command=bq_query_start + "'" + final_transform_sql + "'", 
            trigger_rule='one_success')
    
        
    create_staging >> create_modeled >> branch
    branch >> load_demo >> branch2
    branch2 >> demo_fix_manu >> join
    branch2 >> demo_fix_age >> join
    branch2 >> demo_age_error >> join
    branch2 >> demo_age_error2 >> join
    branch2 >> demo_age_error3 >> join
    branch2 >> demo_date_fix >> join
    branch2 >> demo_date_fix2 >> join
    branch2 >> demo_date_fix3 >> join
    branch2 >> demo_date_fix4 >> join
    branch2 >> demo_date_fix5 >> join
    branch2 >> demo_date_fix6 >> join
    branch >> load_drug >> drug_ing_fix >> join
    branch >> load_outcome >> join
    branch >> load_reaction >> join
    branch >> load_indication >> join
    join >> demo_pat_split >> join3
    join >> demo_ae_split >> join3
    join >> demo_manu_split >> demo_case_split >> reac_pk >> join3
    join >> drug_uni_split >> join2
    join >> drug_ai_split >> join2
    join2 >> drug_admin_split >> create_diag >> join3
    join3 >> inter_table_1 >> join4
    join3 >> inter_table_2 >> join4
    join3 >> inter_table_3 >> join4
    join3 >> inter_table_4 >> join4
    join3 >> inter_table_5 >> join4
    join4 >> final_transform