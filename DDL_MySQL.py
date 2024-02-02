import mysql.connector
from dotenv import load_dotenv
import os

def ddl():

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

    #DIM TABLE
    mysql_cursor.execute("CREATE TABLE IF NOT EXISTS dim_province (kode_prov INT PRIMARY KEY, nama_prov VARCHAR(55))")
    mysql_cursor.execute("CREATE TABLE IF NOT EXISTS dim_district (kode_kab INT PRIMARY KEY, nama_kab VARCHAR(55), kode_prov INT)")
    mysql_cursor.execute("CREATE TABLE IF NOT EXISTS dim_case (case_id INT PRIMARY KEY, status_name VARCHAR(20), status_detail VARCHAR(20))")

    #FACT TABLE
    mysql_cursor.execute("CREATE TABLE IF NOT EXISTS province_daily (id INT PRIMARY KEY, tanggal DATE, kode_prov INT, case_id INT, total INT)")
    mysql_cursor.execute("CREATE TABLE IF NOT EXISTS province_monthly (id INT PRIMARY KEY, tahun INT, bulan INT, kode_prov INT, case_id INT, total INT)")
    mysql_cursor.execute("CREATE TABLE IF NOT EXISTS province_yearly (id INT PRIMARY KEY, tahun INT, kode_prov INT, case_id INT, total INT)")
    mysql_cursor.execute("CREATE TABLE IF NOT EXISTS district_monthly (id INT PRIMARY KEY, tahun INT, bulan INT, kode_kab INT, case_id INT, total INT)")
    mysql_cursor.execute("CREATE TABLE IF NOT EXISTS district_yearly (id INT PRIMARY KEY, tahun INT, kode_kab INT, case_id INT, total INT)")
    
ddl()