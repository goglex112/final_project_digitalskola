from transform import transform_data
from extract import extract_raw_data
import pandas as pd

raw_data = extract_raw_data()
_, _, _, transformed_data = transform_data(raw_data)

print(transformed_data)

def aggregate_province_daily(transformed_data):
    province_daily_df = transformed_data.groupby(by=["tanggal", "kode_prov", "case_id"]).sum("total")
    province_daily_df = province_daily_df.reset_index()

    return province_daily_df

def aggregate_province_monthly(transformed_data):
    province_monthly_df = transformed_data.groupby(by=["tahun", "bulan", "kode_prov", "case_id"]).sum("total")
    province_monthly_df = province_monthly_df.reset_index()

    return province_monthly_df

def aggregate_province_yearly(transformed_data):
    province_yearly_df = transformed_data.groupby(by=["tahun", "kode_prov", "case_id"]).sum("total")
    province_yearly_df = province_yearly_df.reset_index()

    return province_yearly_df

def aggregate_district_monthly(transformed_data):
    district_monthly_df = transformed_data.groupby(by=["tahun", "bulan", "kode_kab", "case_id"]).sum("total")
    district_monthly_df = district_monthly_df.reset_index()

    return district_monthly_df

def aggregate_district_yearly(transformed_data):
    district_yearly_df = transformed_data.groupby(by=["tahun", "kode_kab", "case_id"]).sum("total")
    district_yearly_df = district_yearly_df.reset_index()

    return district_yearly_df
