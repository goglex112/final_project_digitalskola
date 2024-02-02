import pandas as pd

def transform_data(data):

    #Create Province & District Table from data
    province_df = data[['kode_prov', 'nama_prov']].drop_duplicates()
    district_df = data[['kode_kab', 'nama_kab', 'kode_prov']].drop_duplicates()

    #Create Dim Case Table
    case_data = [[1, "suspect", "suspect_diisolasi"], [2, "suspect", "suspect_discarded"], [3, "suspect", "suspect_meninggal"],
                [4, "closecontact", "closecontact_dikarantina"],[5, "closecontact", "closecontact_discarded"],[6, "closecontact", "closecontact_meninggal"],
                [7, "probable", "probable_diisolasi"],[8, "probable", "probable_discarded"],[9, "probable", "probable_meninggal"],
                [10, "confirmation", "confirmation_sembuh"],[11, "confirmation", "confirmation_meninggal"]]

    case_df = pd.DataFrame(case_data, columns=['case_id', 'status_name', 'status_detail'])

    #Create Year & Month column from date column
    data['tanggal'] = pd.to_datetime(data['tanggal'])
    data['tahun'] = data['tanggal'].dt.year 
    data['bulan'] = data['tanggal'].dt.month 


    #Transform data into unpivotted formats
    melted_data = pd.melt(data, id_vars=["tahun", "bulan", "tanggal", "kode_prov", "kode_kab"], 
                value_vars=["suspect_diisolasi", "suspect_discarded", "suspect_meninggal",
                            "closecontact_dikarantina", "closecontact_discarded", "closecontact_meninggal",
                            "probable_diisolasi", "probable_discarded", "probable_meninggal",
                            "confirmation_sembuh", "confirmation_meninggal"], var_name="case_id", value_name="total")

    # Replace the value_vars with case IDs using the case_id mapping
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

    melted_data['case_id'] = melted_data['case_id'].replace(mapping_case_id)

    return province_df, district_df, case_df, melted_data






