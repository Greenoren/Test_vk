from datetime import timedelta
import pandas as pd
import os
import pendulum
import chardet
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

DIR = '/opt/airflow/smb/Airflow_test/Test'
DATA_DIR = os.path.join(DIR, 'input')
DAILY_DIR = os.path.join(DIR, 'daily')
OUTPUT_DIR = os.path.join(DIR, 'output')


def detect_encoding(path: str) -> str:
    with open(path, 'rb') as f:
        raw_data = f.read(10000)
        result = chardet.detect(raw_data)
        encoding = result['encoding']
    return encoding


def create_daily_aggregates(**kwargs):
    dag_run = kwargs['dag_run']
    dt = pendulum.datetime(dag_run.logical_date.year, dag_run.logical_date.month, dag_run.logical_date.day)

    start_date = dt.subtract(weeks=1).date()
    end_date = dt.subtract(days=1).date()

    for single_date in pd.date_range(start=start_date, end=end_date):
        date_str = single_date.strftime("%Y-%m-%d")
        daily_file_path = os.path.join(DATA_DIR, f"{date_str}.csv")
        aggregated_file_path = os.path.join(DAILY_DIR, f"{date_str}_daily.csv")

        encoding = None

        if os.path.exists(aggregated_file_path):
            print(f"Сагрегированный файл за {date_str} уже существует, пропускаем.")
            continue
        else:
            if os.path.exists(daily_file_path):
                if encoding is None:
                    encoding = detect_encoding(daily_file_path)
                df = pd.read_csv(daily_file_path, header=None, names=['email', 'action', 'dt'], sep=',', encoding=encoding)

                aggregated_data = df.groupby(['email', 'action']).size().unstack(fill_value=0)
                all_actions = ['CREATE', 'READ', 'UPDATE', 'DELETE']
                aggregated_data = aggregated_data.reindex(columns=all_actions, fill_value=0)
                aggregated_data.columns = ['create_count', 'read_count', 'update_count', 'delete_count']
                aggregated_data.reset_index(inplace=True)

                output_file_path = os.path.join(DAILY_DIR, f"{single_date.strftime('%Y-%m-%d')}_daily.csv")
                aggregated_data.to_csv(output_file_path, index=False)
                print(f"Сагрегированные данные записаны в {output_file_path}")
            else:
                print(f"Файл данных за {date_str} не найден.")


def aggregate_weekly_data(**kwargs):
    dag_run = kwargs['dag_run']
    dt = pendulum.datetime(dag_run.logical_date.year, dag_run.logical_date.month, dag_run.logical_date.day)
    start_date = dt.subtract(weeks=1).date()  # Начало недели (7 дней назад)
    end_date = dt.subtract(days=1).date()
    output_file_path = os.path.join(OUTPUT_DIR, f"{dt.strftime('%Y-%m-%d')}.csv")

    if os.path.exists(output_file_path):
        print(f"Конечный файл за {dt.strftime('%Y-%m-%d')} уже существует, пропускаем.")
        raise AirflowSkipException("Конечный файл уже существует, пропускаем.")

    all_dfs = []
    for single_date in pd.date_range(start=start_date, end=end_date):
        date_str = single_date.strftime("%Y-%m-%d")
        daily_file_path = os.path.join(DAILY_DIR, f"{date_str}_daily.csv")
        if os.path.exists(daily_file_path):
            df = pd.read_csv(daily_file_path)
            all_dfs.append(df)
        else:
            print(f"Файл данных за {date_str} не найден.")

    if all_dfs:
        aggregated_df = pd.concat(all_dfs).groupby('email').sum().reset_index()
        output_file_path = os.path.join(OUTPUT_DIR, f"{dt.strftime('%Y-%m-%d')}.csv")
        aggregated_df.to_csv(output_file_path, index=False)
        print(f"Итоговые агрегированные данные записаны в {output_file_path}")
    else:
        print("Нет данных для итоговой агрегации.")

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2024, 9, 16),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Vpp_test',
    default_args=default_args,
    description='Test DAG for aggregating',
    schedule='0 7 * * *',
    # schedule=None
)

create_daily_task = PythonOperator(
    task_id='create_daily_aggregates',
    python_callable=create_daily_aggregates,
    provide_context=True,
    dag=dag,
)

aggregate_weekly_data_task = PythonOperator(
    task_id='aggregate_weekly_data',
    python_callable=aggregate_weekly_data,
    provide_context=True,
    dag=dag,
)

create_daily_task >> aggregate_weekly_data_task