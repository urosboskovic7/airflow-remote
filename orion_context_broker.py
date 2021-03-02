from datetime import timedelta
import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from custom_operators.orion_operators import CreateOCBEntityOperator , DeleteOCBEntityOperator, UpdateOCBEntityOperator, ListOCBEntitiesOperator



folder_path = "/opt/airflow/dags_data/ocb_data"
output_path = os.path.join(folder_path, "output_file.txt")


def write_text_file(output_path, **context):

    response = context['task_instance'].xcom_pull(task_ids='list_entities')

    f = open(output_path, "w")
    f.write(str(response))
    f.close()

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

entity1 = {
  "id": "Room1",
  "type": "Room",
  "temperature": {
    "value": 23,
    "type": "Float"
  },
  "pressure": {
    "value": 720,
    "type": "Integer"
  }
}

entity2 = {
  "id": "Room2",
  "type": "Room",
  "temperature": {
    "value": 23,
    "type": "Float"
  },
  "pressure": {
    "value": 720,
    "type": "Integer"
  }
}

attributes1 = {
  "temperature": {
    "value": 26.5,
    "type": "Float"
  },
  "pressure": {
    "value": 763,
    "type": "Float"
  }
}

with DAG('orion_context_broker_dag',
         max_active_runs=3,
         schedule_interval='@once',
         default_args=default_args) as dag:

    create_entity_1 = CreateOCBEntityOperator(task_id='create_entity_1', entity=entity1, fiware_service="airflowtest",fiware_servicepath="/airflowtest")
    create_entity_2 = CreateOCBEntityOperator(task_id='create_entity_2', entity=entity2, fiware_service="airflowtest",fiware_servicepath="/airflowtest")
    update_entity_1 = UpdateOCBEntityOperator(task_id='update_entity_1', attributes=attributes1, entity_id="Room1", fiware_service="airflowtest",fiware_servicepath="/airflowtest")
    delete_entity_2 = DeleteOCBEntityOperator(task_id='delete_entity_2', entity_id="Room2", fiware_service="airflowtest",fiware_servicepath="/airflowtest")
    list_entities = ListOCBEntitiesOperator(task_id='list_entities', fiware_service="airflowtest",fiware_servicepath="/airflowtest")
    write_entities = PythonOperator(task_id="write_entities", python_callable=write_text_file, provide_context=True,
                             op_kwargs={'output_path': output_path})
    delete_updated_entity_1 = DeleteOCBEntityOperator(task_id='delete_updated_entity_1', entity_id="Room1", fiware_service="airflowtest",fiware_servicepath="/airflowtest")

    [create_entity_1, create_entity_2] >> update_entity_1
    update_entity_1 >> delete_entity_2
    delete_entity_2 >> list_entities
    list_entities >> write_entities
    write_entities >> delete_updated_entity_1