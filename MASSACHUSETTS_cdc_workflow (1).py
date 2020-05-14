import datetime
from airflow import models
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date   
    'start_date': datetime.datetime(2020, 5, 4)
    }

'''dag = DAG(
    default_args=default_dag_args,
    description='Draft version of the cdc airflow workflow',
    # Continue to run DAG once per day
    schedule_interval=None,
)'''

staging_dataset = 'MASSACHUSETTS_cdc_workflow_staging'
modeled_dataset = 'MASSACHUSETTS_cdc_workflow_modeled'

staging_dataset_MASSACHUSETTS_cdc_workflow_DEMOGRAPHIC_2011 = 'staging_dataset_MASSACHUSETTS_cdc_workflow_DEMOGRAPHIC_2011'
staging_dataset_MASSACHUSETTS_cdc_workflow_DEMOGRAPHIC_2012 = 'staging_dataset_MASSACHUSETTS_cdc_workflow_DEMOGRAPHIC_2012'
staging_dataset_MASSACHUSETTS_cdc_workflow_DEMOGRAPHIC_2013 = 'staging_dataset_MASSACHUSETTS_cdc_workflow_DEMOGRAPHIC_2013'
staging_dataset_MASSACHUSETTS_cdc_workflow_DEMOGRAPHIC = 'staging_dataset_MASSACHUSETTS_cdc_workflow_DEMOGRAPHIC'

staging_dataset_MASSACHUSETTS_cdc_workflow_OVERALL_Health_Status_2011 = 'staging_dataset_MASSACHUSETTS_cdc_workflow_OVERALL_Health_Status_2011'
staging_dataset_MASSACHUSETTS_cdc_workflow_OVERALL_Health_Status_2012 = 'MASSACHUSETTS_cdc_workflow_OVERALL_Health_Status_2012'
staging_dataset_MASSACHUSETTS_cdc_workflow_OVERALL_Health_Status_2013 = 'MASSACHUSETTS_cdc_workflow_OVERALL_Health_Status_2013'
staging_dataset_MASSACHUSETTS_cdc_workflow_OVERALL_Health_Status = 'MASSACHUSETTS_cdc_workflow_OVERALL_Health_Status'

staging_dataset_MASSACHUSETTS_cdc_workflow_TOBACCO_USE_2011 = 'staging_dataset_MASSACHUSETTS_cdc_workflow_TOBACCO_USE_2011'
staging_dataset_MASSACHUSETTS_cdc_workflow_TOBACCO_USE_2012 = 'staging_dataset_MASSACHUSETTS_cdc_workflow_TOBACCO_USE_2012'
staging_dataset_MASSACHUSETTS_cdc_workflow_TOBACCO_USE_2013 = 'staging_dataset_MASSACHUSETTS_cdc_workflow_TOBACCO_USE_2013'
staging_dataset_MASSACHUSETTS_cdc_workflow_TOBACCO_USE = 'staging_dataset_MASSACHUSETTS_cdc_workflow_TOBACCO_USE'

bq_query_start = 'bq query --use_legacy_sql=false '

create_staging_dataset = 'create or replace table ' + staging_dataset + '''.staging_dataset as
                    select * 
                    from ''' + staging_dataset + '''.staging_dataset'''


create_MASSACHUSETTS_cdc_workflow_DEMOGRAPHIC_2011 = 'create or replace table ' + staging_dataset_MASSACHUSETTS_cdc_workflow_DEMOGRAPHIC_2011 + '''.Takes as
                    select SEQNO, _STATE, SEX, MARITAL, EDUCA, EMPLOY, CHILDREN, INCOME2, WEIGHT2, HEIGHT3
                    from ''' + staging_dataset + '''.2011
                    where _STATE=25'''

create_MASSACHUSETTS_cdc_workflow_DEMOGRAPHIC_2012 = 'create or replace table ' + staging_dataset_MASSACHUSETTS_cdc_workflow_DEMOGRAPHIC_2012 + '''.Takes as
                    select SEQNO, _STATE, SEX, MARITAL, EDUCA, EMPLOY, CHILDREN, INCOME2, WEIGHT2, HEIGHT3
                    from ''' + staging_dataset + '''.2012
                    where _STATE=25'''

create_MASSACHUSETTS_cdc_workflow_DEMOGRAPHIC_2013 = 'create or replace table ' + staging_dataset_MASSACHUSETTS_cdc_workflow_DEMOGRAPHIC_2013 + '''.Takes as
                    select SEQNO, _STATE, SEX, MARITAL, EDUCA, EMPLOY, CHILDREN, INCOME2, WEIGHT2, HEIGHT3
                    from ''' + staging_dataset + '''.2013
                    where _STATE=25'''

create_MASSACHUSETTS_cdc_workflow_OVERALL_Health_Status_2011 = 'create or replace table ' + staging_dataset_MASSACHUSETTS_cdc_workflow_OVERALL_Health_Status_2011 + '''.Takes as
                    select SEQNO, _STATE, GENHLTH, PHYSHLTH, MENTHLTH
                    from ''' + staging_dataset + '''.2011
                    _STATE=25'''

create_MASSACHUSETTS_cdc_workflow_OVERALL_Health_Status_2012 = 'create or replace table ' + staging_dataset_MASSACHUSETTS_cdc_workflow_OVERALL_Health_Status_2012 + '''.Takes as
                    select SEQNO, _STATE, GENHLTH, PHYSHLTH, MENTHLTH
                    from ''' + staging_dataset + '''.2012
                    _STATE=25'''

create_MASSACHUSETTS_cdc_workflow_OVERALL_Health_Status_2013 = 'create or replace table ' + staging_dataset_MASSACHUSETTS_cdc_workflow_OVERALL_Health_Status_2013 + '''.Takes as
                    select SEQNO, _STATE, GENHLTH, PHYSHLTH, MENTHLTH
                    from ''' + staging_dataset + '''.2013
                    _STATE=25'''

create_MASSACHUSETTS_cdc_workflow_TOBACCO_USE_2011 = 'create or replace table ' + staging_dataset_MASSACHUSETTS_cdc_workflow_TOBACCO_USE_2011  + '''.Takes as
                    select SEQNO, _STATE, GENHLTH, PHYSHLTH, MENTHLTH
                    from ''' + staging_dataset + '''.2011
                    _STATE=25'''

create_MASSACHUSETTS_cdc_workflow_OVERALL_TOBACCO_USE_2012 = 'create or replace table ' + staging_dataset_MASSACHUSETTS_cdc_workflow_TOBACCO_USE_2012 + '''.Takes as
                    select SEQNO, _STATE, SMOKE100, SMOKDAY2, LASTSMK2, USENOW3
                    from ''' + staging_dataset + '''.2012
                    where _STATE=25'''

create_MASSACHUSETTS_cdc_workflow_OVERALL_TOBACCO_USE_2013 = 'create or replace table ' + staging_dataset_MASSACHUSETTS_cdc_workflow_TOBACCO_USE_2012 + '''.Takes as
                    select SEQNO, _STATE, SMOKE100, SMOKDAY2, LASTSMK2, USENOW3
                    from ''' + staging_dataset + '''.2013
                    where _STATE=25'''

create_MASSACHUSETTS_cdc_DEMOGRAPHIC = 'create or replace table ' + staging_dataset_MASSACHUSETTS_cdc_DEMOGRAPHIC + '''
select * from(
select SEQNO, _STATE, SEX, MARITAL, EDUCA, EMPLOY, CHILDREN, INCOME2, WEIGHT2, HEIGHT3
from ''' + staging_dataset + '''.staging_dataset_MASSACHUSETTS_cdc_workflow_DEMOGRAPHIC_2011
UNION ALL
select SEQNO, _STATE, SEX, MARITAL, EDUCA, EMPLOY, CHILDREN, INCOME2, WEIGHT2, HEIGHT3
from ''' + staging_dataset + '''.staging_dataset_MASSACHUSETTS_cdc_workflow_DEMOGRAPHIC_2012 
UNION ALL
select SEQNO, _STATE, SEX, MARITAL, EDUCA, EMPLOY1, CHILDREN, INCOME2, WEIGHT2, HEIGHT3
from ''' + staging_dataset + '''.staging_dataset_MASSACHUSETTS_cdc_workflow_DEMOGRAPHIC_2013)'''

create_MASSACHUSETTS_cdc_OVERALL_Health_Status = 'create or replace table ' + '''staging_dataset_MASSACHUSETTS_cdc_OVERALL_Health_Status AS
select * from(
select SEQNO, _STATE, GENHLTH, PHYSHLTH, MENTHLTH
from ''' + staging_dataset + '''.staging_dataset_MASSACHUSETTS_cdc_workflow_OVERALL_Health_Status_2011
UNION ALL
select SEQNO, _STATE, GENHLTH, PHYSHLTH, MENTHLTH
from ''' + staging_dataset + '''.staging_dataset_MASSACHUSETTS_cdc_workflow_OVERALL_Health_Status_2012 
UNION ALL
select SEQNO, _STATE, GENHLTH, PHYSHLTH, MENTHLTH
from ''' + staging_dataset + '''.staging_dataset_MASSACHUSETTS_cdc_workflow_OVERALL_Health_Status_2013)'''

create_MASSACHUSETTS_cdc_TOBACCO_USE = 'create or replace table ' + '''staging_dataset_MASSACHUSETTS_cdc_TOBACCO_USE AS
select * from(
select SEQNO, _STATE, SMOKE100, SMOKDAY2, LASTSMK2, USENOW3
from ''' + staging_dataset + '''.MASSACHUSETTS_TOBACCO_USE_2011
UNION ALL
select SEQNO, _STATE, SMOKE100, SMOKDAY2, LASTSMK2, USENOW3
from ''' + staging_dataset + '''.MASSACHUSETTS_cdc_TOBACCO_USE_2012 
UNION ALL
select SEQNO, _STATE, SMOKE100, SMOKDAY2, LASTSMK2, USENOW3
from ''' + staging_dataset + '''.MASSACHUSETTS_cdc_TOBACCO_USE_2013)'''

#still need to cast to ints? 


with models.DAG(
        'MASSACHUSETTS_cdc_workflow',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    create_staging_dataset = BashOperator(
            task_id='create_staging_dataset',
            bash_command='bq --location=US mk --dataset MASSACHUSETTS_cdc_staging') 
        #does not have dataset_id = "MASSACHUSETTS_cdc_modeled"
  
    load_MASSACHUSETTS_2011 = BashOperator(
            task_id='load_MASSACHUSETTS_2011',
            bash_command='''bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV 
                         gs://the-data-miners-cdc-bucket/the-data-miners-cdc-folder/2011.csv''',
            trigger_rule='one_success')
    
    load_MASSACHUSETTS_2012 = BashOperator(
            task_id='load_MASSACHUSETTS_2012',
            bash_command='''bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV gs://the-data-miners-cdc-bucket/the-data-miners-cdc-folder/2012.csv''',
            trigger_rule='one_success')
    
    load_MASSACHUSETTS_2013 = BashOperator(
            task_id='load_MASSACHUETTS_2013',
            bash_command='''bq --location=US load --autodetect --skip_leading_rows=1 \
                         --source_format=CSV gs://the-data-miners-cdc-bucket/the-data-miners-cdc-folder/2013.csv''', 
            trigger_rule='one_success')
   
    branch1 = DummyOperator(
            task_id='branch1',
            trigger_rule='all_done')
    
    branch2 = DummyOperator(
            task_id='branch2',
            trigger_rule='all_done')
    
    create_MASSACHUSETTS_cdc_workflow_modeled = BashOperator(
            task_id='create_MASSACHUSETTS_cdc_workflow_modeled',
            bash_command='bq --location=US mk --dataset ' + modeled_dataset)
    
    join1 = DummyOperator(
            task_id='join',
            trigger_rule='all_done')
    
    join2 = DummyOperator(
            task_id='join',
            trigger_rule='all_done')
    
    create_DEMOGRAPHIC_2011 = BashOperator(
            task_id='create_DEMOGRAPHIC_2011',
            bash_command=bq_query_start + "'" + create_MASSACHUSETTS_cdc_workflow_DEMOGRAPHIC_2011 + "'", 
            trigger_rule='one_success')
    
    create_DEMOGRAPHIC_2012 = BashOperator(
            task_id='create_DEMOGRAPHIC_2012',
            bash_command=bq_query_start + "'" + create_MASSACHUSETTS_cdc_workflow_DEMOGRAPHIC_2012 + "'", 
            trigger_rule='one_success')
    
    create_DEMOGRAPHIC_2013 = BashOperator(
            task_id='create_DEMOGRAPHIC_2013',
            bash_command=bq_query_start + "'" + create_MASSACHUSETTS_cdc_workflow_DEMOGRAPHIC_2012 + "'", 
            trigger_rule='one_success')
    
    MASSACHUSETTS_DEMOGRAPHIC = BashOperator(
            task_id='MASSACHUSETTS_DEMOGRAPHIC',
            bash_command=bq_query_start + "'" + create_student_sql + "'", 
            trigger_rule='one_success')
    
    MASSACHUSETTS_DEMOGRAPHIC_Beam_DF = BashOperator(
            task_id='MASSACHUSETTS_DEMOGRAPHIC_Beam_DF',
            bash_command='python home/jupyter/airflow/dags/MASSACHUSETTS_DEMOGRAPHIC_beam_dataflow.py', 
            trigger_rule='one_success') #need to pull in the python file
    
    create_OVERALL_Health_Status_2011 = BashOperator(
            task_id='create_OVERALL_Health_Status_2011',
            bash_command=bq_query_start + "'" + create_MASSACHUSETTS_cdc_workflow_OVERALL_Health_Status_2011 + "'", 
            trigger_rule='one_success')
    
    create_OVERALL_Health_Status_2012 = BashOperator(
            task_id='create_OVERALL_Health_Status_2012',
            bash_command=bq_query_start + "'" + create_MASSACHUSETTS_cdc_workflow_OVERALL_Health_Status_2012 + "'", 
            trigger_rule='one_success')
    
    create_OVERALL_Health_Status_2013 = BashOperator(
            task_id=' create_OVERALL_Health_Status_2013',
            bash_command=bq_query_start + "'" + create_MASSACHUSETTS_cdc_workflow_OVERALL_Health_Status_2012 + "'", 
            trigger_rule='one_success')
    
    MASSACHUSETTS_OVERALL_Health_Status = BashOperator(
            task_id='MASSACHUSETTS_OVERALL_Health_Status',
            bash_command=bq_query_start + "'" + create_class_sql + "'", 
            trigger_rule='one_success')
        
    MASSACHUSETTS_OVERALL_Health_Status_Beam_DF = BashOperator(
            task_id='MASSACHUSETTS_OVERALL_Health_Status_Beam_DF',
            bash_command='python home/jupyter/airflow/dags/MASSACHUSETTS_OVERALL_Health_Status_beam_dataflow.py', 
            trigger_rule='one_success') #pull in python file
    
    create_TOBACCO_USE_2011 = BashOperator(
            task_id='create_TOBACCO_USE_2011',
            bash_command=bq_query_start + "'" + create_takes_sql + "'", 
            trigger_rule='one_success')
    
    create_TOBACCO_USE_2012 = BashOperator(
            task_id='create_TOBACCO_USE_2012',
            bash_command=bq_query_start + "'" + create_takes_sql + "'", 
            trigger_rule='one_success')
    
    create_TOBACCO_USE_2013 = BashOperator(
            task_id='create_TOBACCO_USE_2013',
            bash_command=bq_query_start + "'" + create_takes_sql + "'", 
            trigger_rule='one_success')
           
    MASSACHUSETTS_TOBACCO_USE = BashOperator(
            task_id='MASSACHUSETTS_TOBACCO_USE',
            bash_command=bq_query_start + "'" + create_teaches_sql + "'", 
            trigger_rule='one_success')
    
    MASSACHUSETTS_TOBACCO_USE_Beam_DF = BashOperator(
            task_id='MASSACHUSETTS_TOBACCO_USE_Beam_DF',
            bash_command='python home/jupyter/airflow/dags/MASSACHUSETTS_TOBACCO_USE_beam_dataflow.py', 
            trigger_rule='one_success') #pull in python file     
    
    create_staging_dataset >> branch1
    branch1 >> load_MASSACHUSETTS_2011 >> join1
    branch1 >> load_MASSACHUSETTS_2012 >> join1
    branch1 >> load_MASSACHUSETTS_2013 >> join1
    branch1 >> create_modeled_dataset >> branch2
    branch2 >> create_DEMOGRAPHIC_2011 >> create_DEMOGRAPHIC_2012 >> create_DEMOGRAPHIC_2013 >> MASSACHUSETTS_DEMOGRAPHIC >> MASSACHUSETTS_DEMOGRAPHIC_Beam_DF
    branch2 >>  create_OVERALL_Health_Status_2011 >> create_OVERALL_Health_Status_2012 >>  create_OVERALL_Health_Status_2013 >> MASSACHUSETTS_OVERALL_Health_Status >> MASSACHUSETTS_OVERALL_Health_Status_Beam_DF
    branch2 >> MASSACHUSETTS_TOBACCO_USE >> MASSACHUSETTS_TOBACCO_USE_Beam_DF
