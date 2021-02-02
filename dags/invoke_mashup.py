from datetime import timedelta
import ast
import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization

curr_file = os.path.realpath(__file__)
folder_path = os.path.abspath(os.path.join(__file__,"../"))


from custom_operators.mashup_operators import InvokeMashupOperatorDefault


folder_path = os.path.join(folder_path, "dme_data")
input_parameters_path = os.path.join(folder_path, "input_parameters.txt")
output_path = os.path.join(folder_path, "output_file.txt")


def read_text_file(input_path):

    f = open(input_path, "r")
    data = f.read()
    data = ast.literal_eval(data)

    return data

input_parameters = read_text_file(input_parameters_path)


def write_text_file(output_path, **context):

    response = context['task_instance'].xcom_pull(task_ids='invoke_mashup')

    f = open(output_path, "w")
    f.write(str(response))
    f.close()

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


with DAG('invoke_mashup_dag',
         max_active_runs=3,
         schedule_interval='@once',
         default_args=default_args) as dag:

    invoke_mashup = InvokeMashupOperatorDefault(task_id='invoke_mashup', data=input_parameters, mashup_id=401)

    write_response = PythonOperator(task_id="print_response", python_callable=write_text_file, provide_context=True,
                             op_kwargs={'output_path': output_path})

    invoke_mashup >> write_response
