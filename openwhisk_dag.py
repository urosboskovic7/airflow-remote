import ast
from datetime import timedelta
import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from custom_operators.openwhisk_operators import InvokeOpenwhiskAction



folder_path = "/opt/airflow/dags_data/openwhisk_data"
city_one_input_path = os.path.join(folder_path, "city_one_input.txt")
city_two_input_path = os.path.join(folder_path, "city_two_input.txt")
output_path = os.path.join(folder_path, "output_file.txt")



def read_text_file(input_path):

    f = open(input_path, "r")
    data = f.read()
    data = ast.literal_eval(data)

    return data


def get_temperature(response):

    response = response.replace("true", "True")
    response = response.replace("false", "False")
    response = ast.literal_eval(response)
    temp = response["response"]["result"]["temp"]

    return temp

def calculate_mean_temperature(**context):

    city_one_response = context['task_instance'].xcom_pull(task_ids='get_weather_city_one')
    city_two_response = context['task_instance'].xcom_pull(task_ids='get_weather_city_two')

    city_one_temperature = get_temperature(city_one_response)
    city_two_temperature = get_temperature(city_two_response)

    mean_temperature = (city_one_temperature + city_two_temperature)*1.0/2

    return mean_temperature


def write_text_file(output_path, **context):

    mean_temp = context['task_instance'].xcom_pull(task_ids='calculate_mean_temperature')

    f = open(output_path, "w")
    f.write("Mean temperatures for two cities  is " + str(mean_temp))
    f.close()


city_one_data = read_text_file(city_one_input_path)
city_two_data = read_text_file(city_two_input_path)

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


with DAG('openwhisk_dag',
         max_active_runs=3,
         schedule_interval='@once',
         default_args=default_args) as dag:

    # get_weather_city_one = GetWheatherOperator(task_id='get_weather_city_one', data=city_one_data)
    # get_weather_city_two = GetWheatherOperator(task_id='get_weather_city_two', data=city_two_data)

    get_weather_city_one = InvokeOpenwhiskAction(task_id='get_weather_city_one', data=city_one_data, action="weather", username="23bc46b1-71f6-4ed5-8c54-816aa4f8c502", password="123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP")
    get_weather_city_two = InvokeOpenwhiskAction(task_id='get_weather_city_two', data=city_two_data, action="weather", username="23bc46b1-71f6-4ed5-8c54-816aa4f8c502", password="123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP")

    calculate_mean_temp = PythonOperator(task_id="calculate_mean_temperature", python_callable=calculate_mean_temperature, provide_context=True)

    write_file = PythonOperator(task_id="write_file", python_callable=write_text_file, provide_context=True,
                                op_kwargs={'output_path': output_path})

    [get_weather_city_one, get_weather_city_two] >> calculate_mean_temp
    calculate_mean_temp >> write_file
