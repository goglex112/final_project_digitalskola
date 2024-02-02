import requests
import json
import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
import psycopg2
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def func_create_mysql_engine():
    
    load_dotenv()
    mysql_password = os.getenv("MYSQL_PASSWORD")
    mysql_engine = create_engine(f"mysql+mysqlconnector://root:{mysql_password}@172.27.0.4:3306/staging_db")

    return mysql_engine

def func_create_postgres_engine():
    
    postgres_password = os.getenv("POSTGRES_PASSWORD")
    postgres_engine = create_engine(f'postgresql://postgres:{postgres_password}@localhost:5432/final_db')

    return postgres_engine

def func_extract_data():

    url = "http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian?level=kab"
    response = requests.get(url)
    response_json = response.json()

    raw_data = response_json["data"]["content"]

    staging_data = []

    for val in raw_data:

        staging_data_dict = {
            "closecontact": val["CLOSECONTACT"],
            "confirmation": val["CONFIRMATION"],
            "probable": val["PROBABLE"],
            "suspect": val["SUSPECT"],
            "closecontact_dikarantina": val["closecontact_dikarantina"],
            "closecontact_discarded": val["closecontact_discarded"],
            "closecontact_meninggal": val["closecontact_meninggal"],
            "confirmation_meninggal": val["confirmation_meninggal"],
            "confirmation_sembuh": val["confirmation_sembuh"],
            "kode_kab": val["kode_kab"],
            "kode_prov": val["kode_prov"],
            "nama_kab": val["nama_kab"],
            "nama_prov": val["nama_prov"],
            "probable_diisolasi": val["probable_diisolasi"],
            "probable_discarded": val["probable_discarded"],
            "probable_meninggal": val["probable_meninggal"],
            "suspect_diisolasi": val["suspect_diisolasi"],
            "suspect_discarded": val["suspect_discarded"],
            "suspect_meninggal": val["suspect_meninggal"],
            "tanggal": val["tanggal"]
        }

        staging_data.append(staging_data_dict)

    staging_df = pd.DataFrame(staging_data)

    return staging_df


def func_load_to_mysql():
    df = func_extract_data()
    engine = func_create_mysql_engine()
    df.to_sql('staging_covid_table', con=engine, if_exists='replace', index=False)


def func_create_dim_table():

    case_data = [[1, "suspect", "suspect_diisolasi"], [2, "suspect", "suspect_discarded"], [3, "suspect", "suspect_meninggal"],
                [4, "closecontact", "closecontact_dikarantina"],[5, "closecontact", "closecontact_discarded"],[6, "closecontact", "closecontact_meninggal"],
                [7, "probable", "probable_diisolasi"],[8, "probable", "probable_discarded"],[9, "probable", "probable_meninggal"],
                [10, "confirmation", "confirmation_sembuh"],[11, "confirmation", "confirmation_meninggal"]]


    case_df = pd.DataFrame(case_data, columns=['case_id', 'status_name', 'status_detail'])

    mysql_engine =func_create_mysql_engine()
    postgres_engine = func_create_postgres_engine()

    query = f"SELECT * FROM staging_covid_table"

    df = pd.read_sql(query, mysql_engine)

    province_df = df[['kode_prov', 'nama_prov']].drop_duplicates()
    district_df = df[['kode_kab', 'nama_kab', 'kode_prov']].drop_duplicates()
    
    case_df.to_sql('dim_case', con=postgres_engine, if_exists='replace', index=False)
    province_df.to_sql('dim_province', con=postgres_engine, if_exists='replace', index=False)
    district_df.to_sql('dim_district', con=postgres_engine, if_exists='replace', index=False)

def func_create_detail_fact_table():

    query = f"SELECT * FROM staging_covid_table"

    engine = func_create_mysql_engine()

    staging_df = pd.read_sql(query, engine)

    mapping_case_id = {
        'suspect_diisolasi': 1,
        'suspect_discarded': 2,
        'suspect_meninggal': 3,
        'closecontact_dikarantina': 4,
        'closecontact_discarded': 5,
        'closecontact_meninggal': 6,
        'probable_diisolasi': 7,
        'probable_discarded': 8,
        'probable_meninggal': 9,
        'confirmation_sembuh': 10,
        'confirmation_meninggal': 11
    }


    staging_df['tanggal'] = pd.to_datetime(staging_df['tanggal'])
    staging_df['tahun'] = staging_df['tanggal'].dt.year 
    staging_df['bulan'] = staging_df['tanggal'].dt.month 


    melted_data = pd.melt(staging_df, id_vars=["tahun", "bulan", "tanggal", "kode_prov", "kode_kab"], 
                value_vars=["suspect_diisolasi", "suspect_discarded", "suspect_meninggal",
                            "closecontact_dikarantina", "closecontact_discarded", "closecontact_meninggal",
                            "probable_diisolasi", "probable_discarded", "probable_meninggal",
                            "confirmation_sembuh", "confirmation_meninggal"], var_name="case_id", value_name="total")

    # Replace the value_vars with case IDs using the mapping
    melted_data['case_id'] = melted_data['case_id'].replace(mapping_case_id)

    return melted_data


def func_aggregate_province_daily():
    postgres_engine = func_create_postgres_engine()
    melted_data = func_create_detail_fact_table()

    province_daily_df = melted_data.groupby(by=["tanggal", "kode_prov", "case_id"]).sum("total")
    province_daily_df = province_daily_df.reset_index()
    province_daily_df.to_sql('aggregate_province_daily', con=postgres_engine, if_exists='replace', index=True, index_label="id")

def func_aggregate_province_monthly():
    postgres_engine = func_create_postgres_engine()
    melted_data = func_create_detail_fact_table()

    province_monthly_df = melted_data.groupby(by=["tahun", "bulan", "kode_prov", "case_id"]).sum("total")
    province_monthly_df = province_monthly_df.reset_index()
    province_monthly_df.to_sql('aggregate_province_monthly', con=postgres_engine, if_exists='replace', index=True, index_label="id")

def func_aggregate_province_yearly():
    postgres_engine = func_create_postgres_engine()
    melted_data = func_create_detail_fact_table()

    province_yearly_df = melted_data.groupby(by=["tahun", "kode_prov", "case_id"]).sum("total")
    province_yearly_df = province_yearly_df.reset_index()
    province_yearly_df.to_sql('aggregate_province_yearly', con=postgres_engine, if_exists='replace', index=True, index_label="id")


def func_aggregate_district_monthly():
    postgres_engine = func_create_postgres_engine()
    melted_data = func_create_detail_fact_table()
    
    district_monthly_df = melted_data.groupby(by=["tahun", "bulan", "kode_kab", "case_id"]).sum("total")
    district_monthly_df = district_monthly_df.reset_index()
    district_monthly_df.to_sql('aggregate_district_monthly', con=postgres_engine, if_exists='replace', index=True, index_label="id")

def func_aggregate_district_yearly():
    postgres_engine = func_create_postgres_engine()
    melted_data = func_create_detail_fact_table()

    district_yearly_df = melted_data.groupby(by=["tahun", "kode_kab", "case_id"]).sum("total")
    district_yearly_df = district_yearly_df.reset_index()
    district_yearly_df.to_sql('aggregate_district_yearly', con=postgres_engine, if_exists='replace', index=True, index_label="id")


with DAG(
    dag_id='etl_covid',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    }
) as dag: 
    begin_task = EmptyOperator(
        task_id = 'start'
    )

    extract_data = PythonOperator(
        task_id = 'extract_data',
        python_callable=func_extract_data
    )

    load_to_mysql = PythonOperator(
        task_id = 'load_to_mysql',
        python_callable=func_load_to_mysql
    )

    create_dim_table = PythonOperator(
        task_id = 'create_dim_table',
        python_callable=func_create_dim_table
    )

    create_detail_fact_table = PythonOperator(
        task_id = 'create_detail_fact_table',
        python_callable=func_create_detail_fact_table
    )

    aggregate_province_daily = PythonOperator(
        task_id = 'aggregate_province_daily',
        python_callable=func_aggregate_province_daily
    )

    aggregate_province_monthly = PythonOperator(
        task_id = 'aggregate_province_monthly',
        python_callable=func_aggregate_province_monthly
    )

    aggregate_province_yearly = PythonOperator(
        task_id = 'aggregate_province_yearly',
        python_callable=func_aggregate_province_yearly
    )

    aggregate_district_monthly = PythonOperator(
        task_id = 'aggregate_district_monthly',
        python_callable=func_aggregate_district_monthly
    )

    aggregate_district_yearly = PythonOperator(
        task_id = 'aggregate_district_yearly',
        python_callable=func_aggregate_district_yearly
    )

    end_task = EmptyOperator(
        task_id = 'end_task'
    )
   
begin_task >> extract_data >> load_to_mysql >> create_dim_table >> create_detail_fact_table
create_detail_fact_table >> aggregate_province_daily >> end_task
create_detail_fact_table >> aggregate_province_monthly >> end_task
create_detail_fact_table >> aggregate_province_yearly >> end_task
create_detail_fact_table >> aggregate_district_monthly >> end_task
create_detail_fact_table >> aggregate_district_yearly >> end_task
