from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from transform import transform_data
from extract import extract_raw_data
from aggregation import aggregate_province_daily, aggregate_province_monthly, aggregate_province_yearly, aggregate_district_monthly, aggregate_district_yearly

raw_data = extract_raw_data()

province_data, _, _, _ = transform_data(raw_data)
_, district_data, _, _ = transform_data(raw_data)
_, _, case_data, _ = transform_data(raw_data)
aggregate_province_daily_data = aggregate_province_daily(raw_data)
aggregate_province_monthly_data = aggregate_province_monthly(raw_data)
aggregate_province_yearly_data = aggregate_province_yearly(raw_data)
aggregate_district_monthly_data = aggregate_district_monthly(raw_data)
aggregate_district_yearly_data = aggregate_district_yearly(raw_data)

def load_data(province, district, case, province_dl, province_ml, province_yl, district_ml, district_yl):

    load_dotenv()
    mysql_password = os.getenv("MYSQL_PASSWORD")

    mysql_engine = create_engine(f"mysql+mysqlconnector://root:{mysql_password}@localhost/staging_db")
    province.to_sql('dim_province', con=mysql_engine, if_exists='replace', index=False)
    district.to_sql('dim_district', con=mysql_engine, if_exists='replace', index=False)
    case.to_sql('dim_case', con=mysql_engine, if_exists='replace', index=False)

print(aggregate_district_yearly_data)