import os
import pandas as pd
from datetime import datetime


"""
Library of functions that are used in covid_use_case dag
"""



def join_tables(table_one_path, table_two_path):

    """
    Function that joins two tables and writes the joined table
    Input: paths of two tables
    Output: path of joined table
    """

    df_a = pd.read_csv(table_one_path)
    df_b = pd.read_csv(table_two_path)

    df_joined = pd.merge(left=df_a, right=df_b, how='left', on='mpi')

    df_joined['dateofbirth'] = pd.to_datetime(df_joined['dateofbirth'])

    days_in_year = 365.2425
    today = datetime.today()
    df_joined['age'] = df_joined['dateofbirth'].apply(
        lambda x: x if pd.isnull(x) else int((today - x).days / days_in_year))

    folder_path = os.path.abspath(os.path.join(table_one_path, os.pardir))
    output_path = os.path.join(folder_path, "exams_regional_info_merged.csv")

    df_joined.to_csv(output_path, index_label=False)

    return output_path


def split_data_on_age(**context):

    """
    Function that pulls table as xcom object created by task 'join_tables' and splits it on age.
    4 tables (1 for each age category are created and written)
    Output of the function is list of paths of created tables
    """

    table_path = context['task_instance'].xcom_pull(task_ids='join_tables')
    df = pd.read_csv(table_path)

    df_1 = df[df["age"].between(0, 20)].copy()
    df_2 = df[df["age"].between(21, 40)].copy()
    df_3 = df[df["age"] >= 41].copy()
    df_4 = df[pd.isnull(df["age"])].copy()

    folder_path = os.path.abspath(os.path.join(table_path, os.pardir))
    output_path_one = os.path.join(folder_path, "first_group.csv")
    output_path_two = os.path.join(folder_path, "second_group.csv")
    output_path_three = os.path.join(folder_path, "third_group.csv")
    output_path_four = os.path.join(folder_path, "fourth_group.csv")

    df_1.to_csv(output_path_one, index_label=False)
    df_2.to_csv(output_path_two, index_label=False)
    df_3.to_csv(output_path_three, index_label=False)
    df_4.to_csv(output_path_four, index_label=False)

    return [output_path_one, output_path_two, output_path_three, output_path_four]


def status_mapping(x):

    """
    This function maps status of infection from italian to english, while blank values are considered as 'UNDETERMINED'
    """

    if x == "POSITIVO":
        return "INFECTED"
    elif x == "NEGATIVO":
        return "SUSPECTED"
    else:
        return "UNDETERMINED"


def calculate_status(table_index,**context):

    """
    Function that calculates status for each patient from given table and writes it as output file.
    Input: context and table_index.  List of paths from task 'split_data' is pulled as xcom object and using loaded
    using table_index.
    Output: output_path of table with calculated status

    """

    list_of_table_paths = context['task_instance'].xcom_pull(task_ids='split_data')
    table_path = list_of_table_paths[table_index]

    df = pd.read_csv(table_path)

    table_name = os.path.split(table_path)[1][:-4]
    new_table_name = table_name + "_with_status.csv"

    folder_path = os.path.abspath(os.path.join(table_path, os.pardir))
    output_path = os.path.join(folder_path, new_table_name)

    df["status"] = df['result'].apply(lambda x: status_mapping(x))

    df = df[["mpi", "name", "surname", "status"]].copy()

    df.to_csv(output_path, index_label=False)

    return output_path



def concat_tables(**context):

    """
    Function concatinates 4 tables. Table paths are pulled from tasks 'calculate_status_one', 'calculate_status_two',
    'calculate_status_three' and 'calculate_status_four'as xcom objects, loaded and then concatinated
    Input: context
    Output: path of concatinated table
    """

    file_one_path = context['task_instance'].xcom_pull(task_ids='calculate_status_one')
    file_two_path = context['task_instance'].xcom_pull(task_ids='calculate_status_two')
    file_three_path = context['task_instance'].xcom_pull(task_ids='calculate_status_three')
    file_four_path = context['task_instance'].xcom_pull(task_ids='calculate_status_four')

    df1 = pd.read_csv(file_one_path)
    df2 = pd.read_csv(file_two_path)
    df3 = pd.read_csv(file_three_path)
    df4 = pd.read_csv(file_four_path)

    df = pd.concat([df1, df2, df3, df4], ignore_index=True)

    df = df.rename(columns={'mpi': "MPI",
                            "name": "NAME",
                            "surname": "SURNAME",
                            "status": "STATUS"})

    folder_path = os.path.abspath(os.path.join(file_one_path, os.pardir))
    final_path = os.path.join(folder_path, "final_output.csv")

    df.to_csv(final_path, index_label=False)
