import requests
import json
import pandas as pd
from google.cloud import bigquery, storage

firm = [202737,203350,311492,837287]
index = 0
data = {}
frn_info = []
bucket_name = 'fca_dev'

# Define the headers with authentication details
headers = {
    "X-Auth-Email": "xxxxxx@gmail.com",
    "X-Auth-Key": "xxxxxxxxxxxx",
    "Content-Type": "application/json"
}

def extract_from_fca():
    while index < len(firm):
        firm_no = firm[index]
        url = f"https://register.fca.org.uk/services/V0.1/Firm/{firm_no}"
    response = requests.get(url, headers=headers)

    if(response.status_code == 200):
        result = json.loads(response.text)
        data[str(firm_no)] = result['Data']
    index += 1

    for number in data:
        for element in data[number]:
            frn_info.append(element)

    df = pd.DataFrame(frn_info)
    data_df = df[['FRN','Status','Organisation Name','Business Type']]



    storage_client = bigquery.Client()

    table_id = "project.fca_dev"
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

    job = storage_client.load_from_dataframe(data_df, table_id, job_config= job_config)

