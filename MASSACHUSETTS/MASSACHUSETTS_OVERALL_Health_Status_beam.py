import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

### The purpose of this beam pipeline is to cleanse our data.
### Some of the values in our data are Null
### We would like to assign the Nulls to a value of 0.
### this pipeline cleanses OVERALL_Health_Status using DirectRunner.

class FormatNULLFn(beam.DoFn):
  def process(self, element):
    MASSACHUSETTS_OVERALL_Health_Status = element
    SEQNO = MASSACHUSETTS_OVERALL_Health_Status.get('SEQNO')
    _STATE = MASSACHUSETTS_OVERALL_Health_Status.get('_STATE')
    GENHLTH = MASSACHUSETTS_OVERALL_Health_Status.get('GENHLTH')
    PHYSHLTH = MASSACHUSETTS_OVERALL_Health_Status.get('PHYSHLTH')
    MENTHLTH = MASSACHUSETTS_OVERALL_Health_Status.get('MENTHLTH')
    
    ### SEQNO and _STATE are never null.
    ### The statements below mean: if any of the following columns have nulls, they will be made 0.
    
    if GENHLTH is None:
        GENHLTH = 0
    if PHYSHLTH is None:
        PHYSHLTH = 0
    if MENTHLTH is None:
        MENTHLTH = 0
    
    ### This function returns a dictionary (in a list) of all the columns.
        
    return [{'SEQNO': SEQNO, '_STATE': _STATE, 'GENHLTH':GENHLTH, 'PHYSHLTH':PHYSHLTH, 'MENTHLTH':MENTHLTH}]
           
def run():
     PROJECT_ID = 'sunny-advantage-266802' # changed to our project's ID on google cloud

     # Project ID is required when using the BQ source
     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options) #call up pipeline options

     # Create beam pipeline using local runner
     p = beam.Pipeline('DirectRunner', options=opts)

     sql = 'SELECT SEQNO, _STATE, GENHLTH, PHYSHLTH, MENTHLTH FROM MASSACHUSETTS_cdc_modeled.MASSACHUSETTS_OVERALL_Health_Status limit 50' #limit pipeline to the first 50 rows of the OVERALL_Health_Status table
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)  #pull from BigQuery
    
     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
        
     print("query_results", query_results)
        
     query_results | 'Write log 1' >> WriteToText('input_OVERALL_Health_Status.txt') #write input of pipeline to a text file called 'input.txt'

     # apply ParDo to assign null values (when question was not applicable to respondant or when the respondant chose not to answer the question) to a value of 0
     formatted_NULL_pcoll = query_results | 'Format NULL values' >> beam.ParDo(FormatNULLFn())

     # write PCollection to output text file called 'output.txt'
     formatted_NULL_pcoll | 'Write log 2' >> WriteToText('output_OVERALL_Health_Status.txt')

     dataset_id = 'MASSACHUSETTS_cdc_modeled' #assign whole table to datset_id
     table_id = 'MASSACHUSETTS_OVERALL_Health_Status_Beam' #assign only OVERALL_Health_Status table to table_id. 
     schema_id = 'SEQNO:INTEGER, _STATE:INTEGER, GENHLTH:INTEGER, PHYSHLTH:INTEGER, MENTHLTH:INTEGER' #set schema and type of each variable

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