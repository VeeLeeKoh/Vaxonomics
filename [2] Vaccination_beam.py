import os, logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class FormatUnknownFn(beam.DoFn):
  def process(self, element):
    # Process to convert V_FUNDBY, VAX_ROUTE and VAX_SITE attributes into standard unknown
    vaccination_record = element
    
    # get needed attributes
    fundby = vaccination_record.get('V_FUNDBY') #UNK
    route = vaccination_record.get('VAX_ROUTE') #UN
    site = vaccination_record.get('VAX_SITE') #UN
    
    # print current attribute values
    print('Current V_FUNDBY, VAX_ROUTE and VAX_SITE: ', fundby, route, site)
    
    # Convert V_FUNDBY value into UNK if empty
    if fundby == None:
        fundby = 'UNK'
    # Convert VAX_ROUTE value into UN if empty
    if route == None:
        route = 'UN'
    # Convert VAX_SITE value into UN if empty
    if site == None:
        site = 'UN'
    
    # print new attribute values
    print('New V_FUNDBY, VAX_ROUTE and VAX_SITE: ', fundby, route, site)
    
    # update vaccination records
    vaccination_record['V_FUNDBY'] = fundby
    vaccination_record['VAX_ROUTE'] = route
    vaccination_record['VAX_SITE'] = site
        
    # return vaccination records
    return [vaccination_record]
       
    
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
     sql = 'SELECT * FROM vaers_modeled.Vaccination limit 50'
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

     input_pcoll = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
        
     # write input PCollection to input.txt
     input_pcoll | 'Write input_pcoll log 1' >> WriteToText('input_vaccination.txt')
        
     # standardize vaccination V_FUNDBY, VAX_ROUTE and VAX_SITE unknown/empty attribute
     formatted_vaccination_pcoll = input_pcoll | 'Format Unknown Values' >> beam.ParDo(FormatUnknownFn())
        
     # write PCollection to log file
     formatted_vaccination_pcoll | 'Write log 2' >> WriteToText('formatted_unknown_pcoll.txt')
     
     
     # specify id and schema
     dataset_id = 'vaers_modeled'
     table_id = 'Vaccination_Beam'
     schema_id = 'VACCINATION_ID:INTEGER, VAERS_ID:INTEGER, VAX_DATE:DATE, VAX_ID:INTEGER, MANU_ID:INTEGER, V_ADMINBY:STRING, V_FUNDBY:STRING, VAX_ROUTE:STRING, VAX_SITE:STRING' 

     # write output PCollection to new BQ table
     formatted_vaccination_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
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