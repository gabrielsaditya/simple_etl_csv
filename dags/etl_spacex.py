from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import os

# Fungsi Extract

#ubah jadi dataframe dari api (api -> dataframe)
def extract_spacex_launch():
    url = "https://api.spacexdata.com/v4/launches"
    response = requests.get(url)
    data = response.json()
    df = pd.json_normalize(data)
    return df

# Fungsi Transform (transform tipis-tipis seleksi data terus kasih tanggal)
def transform_spacex_data():
    df = extract_spacex_launch()
    selected = df[['name', 'date_utc', 'rocket', 'success', 'details']]
    selected['date_utc'] = pd.to_datetime(selected['date_utc'])
    return selected

# Fungsi Load (load ke csv outputnya di ./dags/output)


def load_to_csv():
    df = transform_spacex_data()
    output_dir = "/opt/airflow/dags/output"
    os.makedirs(output_dir, exist_ok=True)  # auto buat folder kalau belum ada
    file_path = os.path.join(output_dir, "spacex_launches.csv")
    df.to_csv(file_path, index=False)
    print(f"Data berhasil disimpan ke {file_path}")


with DAG(
    dag_id="spacex_etl",
    start_date=datetime(2025, 8, 26),
    schedule="@daily",  # jalan sekali sehari
    catchup=False,
    tags=["ETL", "spacex"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract_spacex_launch
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_spacex_data
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load_to_csv
    )

    # urutan ETL
    extract_task >> transform_task >> load_task