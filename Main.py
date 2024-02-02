import requests
import json
import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine


url = "http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian?level=kab"
response = requests.get(url)
response_json = response.json()

raw_data = response_json["data"]["content"]

case_data = []
province_data = []
district_data = []
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


case_data = [[1, "suspect", "suspect_diisolasi"], [2, "suspect", "suspect_discarded"], [3, "suspect", "suspect_meninggal"],
             [4, "closecontact", "closecontact_dikarantina"],[5, "closecontact", "closecontact_discarded"],[6, "closecontact", "closecontact_meninggal"],
             [7, "probable", "probable_diisolasi"],[8, "probable", "probable_discarded"],[9, "probable", "probable_meninggal"],
             [10, "confirmation", "confirmation_sembuh"],[11, "confirmation", "confirmation_meninggal"]]


case_df = pd.DataFrame(case_data, columns=['case_id', 'status_name', 'status_detail'])
staging_df = pd.DataFrame(staging_data)
province_df = staging_df[['kode_prov', 'nama_prov']].drop_duplicates()
district_df = staging_df[['kode_kab', 'nama_kab', 'kode_prov']].drop_duplicates()

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


province_daily_df = melted_data.groupby(by=["tanggal", "kode_prov", "case_id"]).sum("total")
province_daily_df = province_daily_df.reset_index()

province_monthly_df = melted_data.groupby(by=["tahun", "bulan", "kode_prov", "case_id"]).sum("total")
province_monthly_df = province_monthly_df.reset_index()

province_yearly_df = melted_data.groupby(by=["tahun", "kode_prov", "case_id"]).sum("total")
province_yearly_df = province_yearly_df.reset_index()

district_monthly_df = melted_data.groupby(by=["tahun", "bulan", "kode_kab", "case_id"]).sum("total")
district_monthly_df = district_monthly_df.reset_index()

district_yearly_df = melted_data.groupby(by=["tahun", "kode_kab", "case_id"]).sum("total")
district_yearly_df = district_yearly_df.reset_index()




load_dotenv()
mysql_user = os.getenv("MYSQL_USER")
mysql_password = os.getenv("MYSQL_PASSWORD")

mysql_conn = mysql.connector.connect(
    host = "localhost",
    database = "staging_db",
    user = mysql_user,
    password = mysql_password
)

mysql_cursor = mysql_conn.cursor()

mysql_cursor.execute("CREATE TABLE IF NOT EXISTS dim_province (kode_prov INT PRIMARY KEY, nama_prov VARCHAR(55))")
mysql_cursor.execute("CREATE TABLE IF NOT EXISTS dim_district (kode_kab INT PRIMARY KEY, nama_kab VARCHAR(55), kode_prov INT)")
mysql_cursor.execute("CREATE TABLE IF NOT EXISTS dim_case (case_id INT PRIMARY KEY, status_name VARCHAR(20), status_detail VARCHAR(20))")


mysql_engine = create_engine(f"mysql+mysqlconnector://root:{mysql_password}@localhost/staging_db")
province_df.to_sql('dim_province', con=mysql_engine, if_exists='replace', index=False)
district_df.to_sql('dim_district', con=mysql_engine, if_exists='replace', index=False)
case_df.to_sql('dim_case', con=mysql_engine, if_exists='replace', index=False)

