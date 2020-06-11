import os, logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class FormatRECOVDFn(beam.DoFn):
  def process(self, element):
    # Process to convert RECOVD attribute into boolean by standarding values into true, false or null
    # Attribute type will be changed to boolean in schema
    event_record = element
    
    # get RECOVD attribute
    recovd = event_record.get('RECOVD')
    
    # print current RECOVD values
    print('Current RECOVD: ', recovd)
    
    # Convert RECOVD value into None if Unknown (U)
    if recovd == 'U':
        recovd = None
    # Convert RECOVD value into True if Yes (Y)
    elif recovd == 'Y':
        recovd = True
    # Convert RECOVD value into False if No (N)
    elif recovd == 'N':
        recovd = False
    
    # print new RECOVD values
    print('New RECOVD: ', recovd)
    
    # update advserse event records
    event_record['RECOVD'] = recovd
        
    # return advserse event records
    return [event_record]


class FormatBIRTH_DEFECTFn(beam.DoFn):
  def process(self, element):
    # Process to convert BIRTH_DEFECT attribute into boolean by standarding values into true or false
    # Attribute type will be changed to boolean in schema
    event_record = element # event_record is an _UnwindowedValues object
    
    # get BIRTH_DEFECT attribute
    birth_defect = event_record.get('BIRTH_DEFECT')
     
    # print current BIRTH_DEFECT values
    print('Current BIRTH_DEFECT: ', birth_defect)
    
    # Convert BIRTH_DEFECT value into False if None (Interpreted as False here as values are either Y or empty)
    if birth_defect == None:
        birth_defect = False
    # Convert BIRTH_DEFECT value into True if Yes (Y)
    elif birth_defect == 'Y':
        birth_defect = True
        
    # print new BIRTH_DEFECT values
    print('New BIRTH_DEFECT: ', birth_defect)
    
    # update advserse event records
    event_record['BIRTH_DEFECT'] = birth_defect
    
    # return adverse event records
    return [event_record]
   
    
class FormatBooleanAttributesFn(beam.DoFn):
  def process(self, element):
    # Process to convert boolean attribute values into false if they are empty or null 
    # This includes the attributes of DIED, L_THREAT, OFC_VISIT, ER_VISIT, ER_ED_VISIT, HOSPITAL, X_STAY and DISABLE
    event_record = element # event_record is an _UnwindowedValues object
    
    # get boolean attributes
    died = event_record.get('DIED')
    l_threat = event_record.get('L_THREAT')
    ofc_visit = event_record.get('OFC_VISIT')
    er_visit = event_record.get('ER_VISIT')
    er_ed_visit = event_record.get('ER_ED_VISIT')
    hospital = event_record.get('HOSPITAL')
    x_stay = event_record.get('X_STAY')
    disable = event_record.get('DISABLE')
     
    # print sample current values
    print('Current DIED: ', died)
    
    # Convert boolean values into False if None (Interpreted as False here as values are either Y or empty)
    # Update adverse event records directly
    if died == None:
        event_record['DIED'] = False
    if l_threat == None:
        event_record['L_THREAT'] = False
    if ofc_visit == None:
        event_record['OFC_VISIT'] = False
    if er_visit == None:
        event_record['ER_VISIT'] = False
    if er_ed_visit == None:
        event_record['ER_ED_VISIT'] = False
    if hospital == None:
        event_record['HOSPITAL'] = False
    if x_stay == None:
        event_record['X_STAY'] = False
    if disable == None:
        event_record['DISABLE'] = False
        
    # print new BIRTH_DEFECT values
    print('New DIED: ', event_record['DIED'])
    
    # return adverse event records
    return [event_record]
       
    
def run():
     PROJECT_ID = 'studied-brand-266702'

     # Project ID is required when using the BQ source
     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     # create beam pipeline using local runner
     p = beam.Pipeline('DirectRunner', options=opts)
     
     # run BigQuery query on dataset
     sql = 'SELECT * FROM vaers_modeled.Adverse_Event limit 50'
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

     input_pcoll = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
        
     # write input PCollection to input.txt
     input_pcoll | 'Write input_pcoll log 1' >> WriteToText('input.txt')
        
     # standardize adverse_event RECOVD attribute values into true or false (boolean)
     formatted_recovd_pcoll = input_pcoll | 'Format RECOVD' >> beam.ParDo(FormatRECOVDFn())
        
     # write PCollection to log file
     formatted_recovd_pcoll | 'Write log 2' >> WriteToText('formatted_recovd_pcoll.txt')
        
     # standardize adverse_event BIRTH_DEFECT attribute values into true or false (boolean)
     formatted_defect_pcoll = formatted_recovd_pcoll | 'Format BIRTH_DEFECT' >> beam.ParDo(FormatBIRTH_DEFECTFn())
        
     # write PCollection to log file
     formatted_defect_pcoll | 'Write log 3' >> WriteToText('formatted_defect_pcoll.txt')
        
     # standardize boolean attribute values which are null into false 
     output_pcoll = formatted_defect_pcoll | 'Format boolean attributes' >> beam.ParDo(FormatBooleanAttributesFn())
        
     # write output PCollection to output.txt
     output_pcoll | 'Write output_pcoll log 4' >> WriteToText('output.txt')
     
     # specify id and schema
     dataset_id = 'vaers_modeled'
     table_id = 'Adverse_Event_Beam'
     # change RECOVD and BIRTH_DEFECT attributes into BOOLEANS
     schema_id = 'VAERS_ID:INTEGER, ONSET_DATE:DATE, RECOVD:BOOLEAN, DIED:BOOLEAN, DATEDIED:DATE, L_THREAT:BOOLEAN, OFC_VISIT:BOOLEAN, ER_VISIT:BOOLEAN, ER_ED_VISIT:BOOLEAN, HOSPITAL:BOOLEAN, HOSPDAYS:INTEGER, X_STAY:BOOLEAN, DISABLE:BOOLEAN, BIRTH_DEFECT:BOOLEAN, OTHER_MEDS:STRING, CUR_ILL:STRING, HISTORY:STRING, PRIOR_VAX:STRING' 

     # write output PCollection to new BQ table
     output_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                  table=table_id, 
                                                  schema=schema_id,
                                                  project=PROJECT_ID,
                                                  create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                  write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                  batch_size=int(100))
         
     result = p.run()
     result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()