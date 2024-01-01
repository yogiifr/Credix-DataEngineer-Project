from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import csv
from time import sleep

from google.cloud import storage
from io import BytesIO
import os 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=".\long-memory-400610-8457d272a99f.json"

def load_avro_schema_from_file():
    key_schema = avro.load("application_record_key.avsc")
    value_schema = avro.load("application_record_value.avsc")

    return key_schema, value_schema

def download_csv_from_gcs(bucket_name, file_path):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_path)
    content = blob.download_as_text()
    return content.splitlines()

def send_record():
    key_schema, value_schema = load_avro_schema_from_file()
    
    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081",
        "acks": "1"
    }

    producer = AvroProducer(producer_config, default_key_schema=key_schema, default_value_schema=value_schema)

    # Update GCS details
    gcs_bucket_name = 'projectcredix_group3_datalake'
    gcs_file_path = 'credit_application/application_record_20231214.csv'
    csv_lines = download_csv_from_gcs(gcs_bucket_name, gcs_file_path)

    header = csv_lines[0].split(';')
    for row in csv_lines[1:]:
        row_data = row.split(';')
        key = {"ID": int(row_data[0])}
        value = {
            "ID": int(row_data[0]),
            "CODE_GENDER": str(row_data[1]),
            "FLAG_OWN_CAR": str(row_data[2]),
            "FLAG_OWN_REALTY": str(row_data[3]),
            "CNT_CHILDREN": int(row_data[4]),
            "AMT_INCOME_TOTAL": float(row_data[5]),
            "NAME_INCOME_TYPE": str(row_data[6]),
            "NAME_EDUCATION_TYPE": str(row_data[7]),
            "NAME_FAMILY_STATUS": str(row_data[8]),
            "NAME_HOUSING_TYPE": str(row_data[9]),
            "DAYS_BIRTH": int(row_data[10]),
            "DAYS_EMPLOYED": int(row_data[11]),
            "FLAG_MOBIL": int(row_data[12]),
            "FLAG_WORK_PHONE": int(row_data[13]),
            "FLAG_PHONE": int(row_data[14]),
            "FLAG_EMAIL": int(row_data[15]),
            "OCCUPATION_TYPE": str(row_data[16]),
            "CNT_FAM_MEMBERS": float(row_data[17])
        }

        try:
            producer.produce(topic='practice.application_record_Training', key=key, value=value)
        except Exception as e:
            print(f"Exception while producing record value - {value}: {e}")
        else:
            print(f"Successfully producing record value - {value}")

        producer.flush()

    # for row in csvreader:
    #         key = {"ID":  int(row[0])}
    #         value = {
    #             "ID": int(row[0]),
    #             "CODE_GENDER": str(row[1]),
    #             "FLAG_OWN_CAR": str(row[2]),
    #             "FLAG_OWN_REALTY": str(row[3]),
    #             "CNT_CHILDREN": int(row[4]),
    #             "AMT_INCOME_TOTAL": float(row[5]),
    #             "NAME_INCOME_TYPE": str(row[6]),
    #             "NAME_EDUCATION_TYPE": str(row[7]),
    #             "NAME_FAMILY_STATUS": str(row[8]),
    #             "NAME_HOUSING_TYPE": str(row[9]),
    #             "DAYS_BIRTH": int(row[10]),
    #             "DAYS_EMPLOYED": int(row[11]),
    #             "FLAG_MOBIL": int(row[12]),
    #             "FLAG_WORK_PHONE": int(row[13]),
    #             "FLAG_PHONE": int(row[14]),
    #             "FLAG_EMAIL": int(row[15]),
    #             "OCCUPATION_TYPE": str(row[16]),
    #             "CNT_FAM_MEMBERS": float(row[17])
    #         }

    #         try:
    #             producer.produce(topic='practice.application_record_Training', key=key, value=value)
    #         except Exception as e:
    #             print(f"Exception while producing record value - {value}: {e}")
    #         else:
    #             print(f"Successfully producing record value - {value}")

    #         producer.flush()

if __name__ == "__main__":
    send_record()
