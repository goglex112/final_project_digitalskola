def load_data():

    
#     load_dotenv()
#     mysql_password = os.getenv("MYSQL_PASSWORD")

#     mysql_engine = create_engine(f"mysql+mysqlconnector://root:{mysql_password}@localhost/staging_db")
#     province_df.to_sql('dim_province', con=mysql_engine, if_exists='replace', index=False)
#     district_df.to_sql('dim_district', con=mysql_engine, if_exists='replace', index=False)
#     case_df.to_sql('dim_case', con=mysql_engine, if_exists='replace', index=False)