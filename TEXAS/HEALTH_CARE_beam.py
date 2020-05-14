import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

### The purpose of this beam pipeline is to cleanse our data.
### Some of the values in our data are Null
### We would like to assign the Nulls to a value of 0.
### This pipeline modifies HEALTH_CARE with a direct runner

class FormatNULLFn(beam.DoFn):
  def process(self, element):
    HEALTH_CARE = element
    SEQNO = HEALTH_CARE.get('SEQNO')
    _STATE = HEALTH_CARE.get('_STATE')
    HLTHPLN1 = HEALTH_CARE.get('HLTHPLN1')
    PERSDOC2 = HEALTH_CARE.get('PERSDOC2')
    MEDCOST = HEALTH_CARE.get('MEDCOST')
    CHECKUP1 = HEALTH_CARE.get('CHECKUP1')
    
    ### SEQNO and _STATE are never null.
    ### The statements below mean: if any of the following columns have nulls, they will be made 0.
    
    if HLTHPLN1 is None:
        HLTHPLN1 = 0
    if PERSDOC2 is None:
        PERSDOC2 = 0
    if MEDCOST is None:
        MEDCOST = 0
    if CHECKUP1 is None:
        CHECKUP1 = 0

    ### This function returns a dictionary (in a list) of all the columns.
        
    return [{'SEQNO': SEQNO, '_STATE': _STATE, 'HLTHPLN1':HLTHPLN1, 'PERSDOC2':PERSDOC2, 'MEDCOST':MEDCOST, 'CHECKUP1':CHECKUP1}]
           
def run():
     PROJECT_ID = 'sunny-advantage-266802' # changed to our project's ID on google cloud

     # Project ID is required when using the BQ source
     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options) #call up pipeline options

     # Create beam pipeline using local runner
     p = beam.Pipeline('DirectRunner', options=opts)

     sql = 'SELECT SEQNO, _STATE, HLTHPLN1, PERSDOC2, MEDCOST, CHECKUP1 FROM cdc_modeled.HEALTH_CARE limit 50' #limit pipeline to the first 50 rows of the Demographics table
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)  #pull from BigQuery

     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
        
     print("query_results", query_results)
        
     query_results | 'Write log 1' >> WriteToText('input_HEALTH_CARE.txt') #write input of pipeline to a text file called 'input.txt'

     # apply ParDo to assign null values (when question was not applicable to respondant or when the respondant chose not to answer the question) to a value of 0
     formatted_NULL_pcoll = query_results | 'Format NULL values' >> beam.ParDo(FormatNULLFn())

     # write PCollection to output text file called 'output.txt'
     formatted_NULL_pcoll | 'Write log 2' >> WriteToText('output_HEALTH_CARE.txt')

     dataset_id = 'cdc_modeled' #assign whole table to datset_id
     table_id = 'HEALTH_CARE_Beam' #assign only demographic table to table_id. 
     schema_id = 'SEQNO:INTEGER, _STATE:INTEGER, HLTHPLN1:INTEGER, PERSDOC2:INTEGER, MEDCOST:INTEGER, CHECKUP1:INTEGER' #set schema and type of each variable

     # write PCollection to new BQ table
     formatted_NULL_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
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
