import os, datetime, logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

### The purpose of this beam pipeline is to cleanse our data.
### Some of the values in our data are Null
### We would like to assign the Nulls to a value of 0.
### this pipeline cleanses OVERALL_Health_Status using DataFlowRunner.

class FormatNULLFn(beam.DoFn):
  def process(self, element):
    OVERALL_Health_Status = element
    SEQNO = OVERALL_Health_Status.get('SEQNO')
    _STATE = OVERALL_Health_Status.get('_STATE')
    GENHLTH = OVERALL_Health_Status.get('GENHLTH')
    PHYSHLTH = OVERALL_Health_Status.get('PHYSHLTH')
    MENTHLTH = OVERALL_Health_Status.get('MENTHLTH')
    
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
    PROJECT_ID = 'sunny-advantage-266802'  # changed to our project's ID on google cloud
    BUCKET = 'gs://beam_cs327e_data_thedataminers' #establishes pathway to bucket in GCP
    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

    # run pipeline on Dataflow 
    options = {
        'runner': 'DataflowRunner',
        'job_name': 'transform-overall-health-status2',
        'project': PROJECT_ID,
        'temp_location': BUCKET + '/temp',
        'staging_location': BUCKET + '/staging',
        'machine_type': 'n1-standard-4', # https://cloud.google.com/compute/docs/machine-types
        'num_workers': 1
    }

    opts = beam.pipeline.PipelineOptions(flags=[], **options) #call up pipeline options

    p = beam.Pipeline('DataflowRunner', options=opts) # Create beam pipeline using local runner

    sql = 'SELECT SEQNO, _STATE, GENHLTH, PHYSHLTH, MENTHLTH FROM cdc_modeled.OVERALL_Health_Status '
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True) #pull from BigQuery

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source) 

    # apply ParDo to assign null values (when question was not applicable to respondant or when the respondant chose not to answer the question) to a value of 0
    query_results | 'Write log 1' >> WriteToText(DIR_PATH + 'query_results.txt')

    # apply ParDo to format the null values  
    formatted_NULL_pcoll = query_results | 'Format NULL values' >> beam.ParDo(FormatNULLFn())

    # write PCollection to output text file
    formatted_NULL_pcoll | 'Write log 2' >> WriteToText(DIR_PATH + 'formatted_NULL_pcoll.txt')

    dataset_id = 'cdc_modeled' #assign whole table to datset_id
    table_id = 'OVERALL_Health_Status_Beam_DF' #assign only demographic table to table_id.
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
