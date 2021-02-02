import os
import sys
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator




curr_file = os.path.realpath(__file__)
folder_path = os.path.abspath(os.path.join(__file__,"../"))

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from dags.config.covid_use_case_library import join_tables, split_data_on_age, calculate_status, concat_tables


folder_path = os.path.join(folder_path, "covid_use_case")
table_a_path = os.path.join(folder_path, "exams.csv")
table_b_path = os.path.join(folder_path, "regional_info.csv")

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}

dag = DAG(
    'covid_use_case',
    default_args=default_args,
    description='Covid use case',
    schedule_interval=timedelta(days=1),
    access_control={
        'DagTest': {'can_dag_read', 'can_dag_edit'}
    }
)


t1 = PythonOperator(
    task_id='join_tables',
    python_callable=join_tables,
    op_kwargs={'table_one_path': table_a_path, 'table_two_path': table_b_path
        #, 'output_path': table_c_path
               },
    dag=dag,
)


t2 = PythonOperator(
    task_id='split_data',
    depends_on_past=False,
    python_callable=split_data_on_age,
    # op_kwargs={'table_path': table_c_path,
    #            'output_path_one': first_group_path,
    #            'output_path_two': second_group_path,
    #            'output_path_three': third_group_path,
    #            'output_path_four': fourth_group_path},
    provide_context=True,
    dag=dag,
)

t3 = PythonOperator(
    task_id='calculate_status_one',
    depends_on_past=False,
    python_callable=calculate_status,
    op_kwargs={'table_index': 0,
               #'output_path': first_group_with_status_path
               },
    provide_context=True,
    dag=dag,
)

t4 = PythonOperator(
    task_id='calculate_status_two',
    depends_on_past=False,
    python_callable=calculate_status,
    op_kwargs={'table_index': 1,
               #'output_path': second_group_with_status_path
               },
    provide_context=True,
    dag=dag,
)

t5 = PythonOperator(
    task_id='calculate_status_three',
    depends_on_past=False,
    python_callable=calculate_status,
    op_kwargs={'table_index': 2,
               #'output_path': third_group_with_status_path
               },
    provide_context=True,
    dag=dag,
)

t6 = PythonOperator(
    task_id='calculate_status_four',
    depends_on_past=False,
    python_callable=calculate_status,
    op_kwargs={'table_index': 3,
               #'output_path': fourth_group_with_status_path
               },
    provide_context=True,
    dag=dag,
)

t7 = PythonOperator(
    task_id='concat_tables',
    depends_on_past=False,
    python_callable=concat_tables,
    # op_kwargs={'file_one_path': first_group_with_status_path,
    #            'file_two_path': second_group_with_status_path,
    #            'file_three_path': third_group_with_status_path,
    #            'file_four_path': fourth_group_with_status_path,
    #            'final_path': final_output_path},
    provide_context=True,
    dag=dag,
)

t1 >> t2
t2 >> [t3, t4, t5, t6]
[t3, t4, t5, t6] >> t7
