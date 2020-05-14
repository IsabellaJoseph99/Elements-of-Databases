import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

### The purpose of this beam pipeline is to cleanse our data.
### We copied the pipeline used in MILESTONE 5 and modified code as necessary
### Some of the values in our data are Null
### We would like to assign the Nulls to a value of 0.
### The table we have chosen is our main table, DEMOGRAPHIC.
### We also converted WEIGHT2 and HEIGHT3 into pounds and inches, respectively, and added error handling for out-of-bounds values in CHILDREN 

class FormatNULLFn(beam.DoFn):
  def process(self, element):
    DEMOGRAPHIC = element
    SEQNO = DEMOGRAPHIC.get('SEQNO')
    _STATE = DEMOGRAPHIC.get('_STATE')
    SEX = DEMOGRAPHIC.get('SEX')
    MARITAL = DEMOGRAPHIC.get('MARITAL')
    EDUCA = DEMOGRAPHIC.get('EDUCA')
    EMPLOY = DEMOGRAPHIC.get('EMPLOY')
    CHILDREN = DEMOGRAPHIC.get('CHILDREN')
    INCOME2 = DEMOGRAPHIC.get('INCOME2')
    WEIGHT2 = DEMOGRAPHIC.get('WEIGHT2')
    HEIGHT3 = DEMOGRAPHIC.get('HEIGHT3')
    
    ### SEQNO and _STATE are never null.
    ### The statements below mean: if any of the following columns have nulls, they will be made 0.
    
    if SEX is None:
        SEX = 0
    if MARITAL is None:
        MARITAL = 0
    if EDUCA is None:
        EDUCA = 0
    if EMPLOY is None:
        EMPLOY = 0
    if CHILDREN is None or (CHILDREN > 20 and CHILDREN != 88 and CHILDREN != 99):
        CHILDREN = 0
    if INCOME2 is None:
        INCOME2 = 0
    if WEIGHT2 is None or WEIGHT2 == 9000 or WEIGHT2 == 9999 or WEIGHT2 == 7777:
        WEIGHT2 = 0
    if WEIGHT2 < 9999 and WEIGHT2 > 9000:
        WEIGHT2 = int((WEIGHT2 - 9000)*2.2)
    if HEIGHT3 is None:
        HEIGHT3 = 0
    if HEIGHT3 <= 711 and HEIGHT3 >= 200: 
        FEET = HEIGHT3 // 100
        INCHES = HEIGHT3 - (FEET * 100)
        HEIGHT3 = int((FEET * 12) + INCHES)
    
    ### This function returns a dictionary (in a list) of all the columns.
        
    return [{'SEQNO': SEQNO, '_STATE': _STATE, 'SEX':SEX, 'MARITAL':MARITAL, 'EDUCA':EDUCA, 'EMPLOY':EMPLOY, 'CHILDREN':CHILDREN, 'INCOME2':INCOME2, 'WEIGHT2':WEIGHT2, 'HEIGHT3':HEIGHT3}]
           
def run():
     PROJECT_ID = 'sunny-advantage-266802' # changed to our project's ID on google cloud

     # Project ID is required when using the BQ source
     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options) #call up pipeline options

     # Create beam pipeline using local runner
     p = beam.Pipeline('DirectRunner', options=opts)

     sql = 'SELECT SEQNO, _STATE, SEX, MARITAL, EDUCA, EMPLOY, CHILDREN, INCOME2, WEIGHT2, HEIGHT3  FROM cdc_modeled.DEMOGRAPHIC limit 50' #limit pipeline to the first 50 rows of the Demographics table
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)  #pull from BigQuery

     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
        
     print("query_results", query_results)
        
     query_results | 'Write log 1' >> WriteToText('input_DEMOGRAPHIC.txt') #write input of pipeline to a text file called 'input.txt'

     # apply ParDo to assign null values (when question was not applicable to respondant or when the respondant chose not to answer the question) to a value of 0
     formatted_NULL_pcoll = query_results | 'Format NULL values' >> beam.ParDo(FormatNULLFn())

     # write PCollection to output text file called 'output.txt'
     formatted_NULL_pcoll | 'Write log 2' >> WriteToText('output_DEMOGRAPHIC.txt')

     dataset_id = 'cdc_modeled' #assign whole table to datset_id
     table_id = 'DEMOGRAPHIC_Beam' #assign only demographic table to table_id. 
     schema_id = 'SEQNO:INTEGER, _STATE:INTEGER, SEX:INTEGER, MARITAL:INTEGER, EDUCA:INTEGER, EMPLOY:INTEGER, CHILDREN:INTEGER, INCOME2:INTEGER, WEIGHT2:INTEGER, HEIGHT3:INTEGER' #set schema and type of each variable

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
