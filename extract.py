import requests
import json
import pandas as pd

def extract_raw_data():

    url = "http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian?level=kab"
    response = requests.get(url)
    response_json = response.json()

    raw_data = response_json["data"]["content"]

    # case_data = []
    # province_data = []
    # district_data = []
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