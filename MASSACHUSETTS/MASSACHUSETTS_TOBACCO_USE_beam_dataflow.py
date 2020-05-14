import os, datetime, logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

### The purpose of this beam pipeline is to cleanse our data.
### Some of the values in our data are Null
### We would like to assign the Nulls to a value of 0.
### This pipeline cleanses TOBACCO_USE with DataFlowRunner

class FormatNULLFn(beam.DoFn):
  def process(self, element):
    MASSACHUSETTS_TOBACCO_USE = element
    SEQNO = MASSACHUSETTS_TOBACCO_USE.get('SEQNO')
    _STATE = MASSACHUSETTS_TOBACCO_USE.get('_STATE')
    SMOKE100 = MASSACHUSETTS_TOBACCO_USE.get('SMOKE100')
    SMOKDAY2 = MASSACHUSETTS_TOBACCO_USE.get('SMOKDAY2')
    LASTSMK2 = MASSACHUSETTS_TOBACCO_USE.get('LASTSMK2')
    USENOW3 = MASSACHUSETTS_TOBACCO_USE.get('USENOW3')
    
    ### SEQNO and _STATE are never null.
    ### The statements below mean: if any of the following columns have nulls, they will be made 0.
    
    if SMOKE100 is None:
        SMOKE100 = 0
    if SMOKDAY2 is None:
        SMOKDAY2 = 0
    if LASTSMK2 is None:
        LASTSMK2 = 0
    if USENOW3 is None:
        USENOW3 = 0

    ### This function returns a dictionary (in a list) of all the columns.
        
    return [{'SEQNO': SEQNO, '_STATE': _STATE, 'SMOKE100':SMOKE100, 'SMOKDAY2':SMOKDAY2, 'LASTSMK2':LASTSMK2, 'USENOW3':USENOW3}]
           
def run():         
    PROJECT_ID = 'sunny-advantage-266802'
    BUCKET = 'gs://beam_cs327e_data_thedataminers'
    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

    # run pipeline on Dataflow 
    options = {
        'runner': 'DataflowRunner',
        'job_name': 'transform-tobacco-use',
        'project': PROJECT_ID,
        'temp_location': BUCKET + '/temp',
        'staging_location': BUCKET + '/staging',
        'machine_type': 'n1-standard-4', # https://cloud.google.com/compute/docs/machine-types
        'num_workers': 1
    }

    opts = beam.pipeline.PipelineOptions(flags=[], **options) #call up pipeline options

    p = beam.Pipeline('DataflowRunner', options=opts) # Create beam pipeline using DataFlowRunner

    sql = 'SELECT SEQNO, _STATE, SMOKE100, SMOKDAY2, LASTSMK2, USENOW3 FROM MASSACHUSETTS_cdc_modeled.MASSACHUSETTS_TOBACCO_USE'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True) #pull from BigQuery

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

    #write input of pipeline to a text file called 'input.txt'
    query_results | 'Write log 1' >> WriteToText(DIR_PATH + 'query_results.txt')

    # apply ParDo to format null values  
    formatted_NULL_pcoll = query_results | 'Format NULL values' >> beam.ParDo(FormatNULLFn())

    # # write PCollection to output text file called 'output.txt'
    formatted_NULL_pcoll | 'Write log 2' >> WriteToText(DIR_PATH + 'formatted_NULL_pcoll.txt')

    dataset_id = 'MASSACHUSETTS_cdc_modeled' #assign whole table to datset_id
    table_id = 'MASSACHUSETTS_TOBACCO_USE_Beam_DF' #assign only TOBACCO_USE table to table_id.
    schema_id = 'SEQNO:INTEGER, _STATE:INTEGER, SMOKE100:INTEGER, SMOKDAY2:INTEGER, LASTSMK2:INTEGER, USENOW3:INTEGER' #set schema and type of each variable

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