from confluent_kafka.avro import AvroConsumer
from google.cloud import bigquery
from google.cloud import storage
from io import BytesIO
from avro import schema as avro_schema, io

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ".\long-memory-400610-8457d272a99f.json"

dataset_name = 'bronze_projectcredix_group3'
table_name = 'application_record'

client = bigquery.Client()
dataset_ref = client.dataset(dataset_name)
dataset = bigquery.Dataset(dataset_ref)

# Create the dataset if it doesn't exist
try:
    client.create_dataset(dataset, exists_ok=True)
except Exception as e:
    print(f"Error creating dataset: {e}")

schema_definition = [
    bigquery.SchemaField('ID', 'INT64'),
    bigquery.SchemaField('CODE_GENDER', 'STRING'),
    bigquery.SchemaField('FLAG_OWN_CAR', 'STRING'),
    # ... (other fields remain unchanged)
]

table_ref = dataset_ref.table(table_name)
table = bigquery.Table(table_ref, schema=schema_definition)

# Create the table if it doesn't exist
try:
    client.create_table(table, exists_ok=True)
except Exception as e:
    print(f"Error creating table: {e}")

def avro_deserializer(serialized_data, avro_schema):
    bytes_io = BytesIO(serialized_data)
    reader = io.DatumReader(avro_schema)
    data = io.BinaryDecoder(bytes_io)
    return reader.read(data)

def download_avro_records_from_gcs(bucket_name, file_path, key_schema, value_schema):
    client = storage.Client()
    blob = client.bucket(bucket_name).blob(file_path)
    avro_bytes = blob.download_as_bytes()

    return AvroConsumer(
        {
            "bootstrap.servers": "localhost:9092",
            "schema.registry.url": "http://localhost:8081",
            "group.id": "application_record.avro.consumer.2",
            "auto.offset.reset": "earliest"
        },
        reader_value_schema=value_schema,
        reader_key_schema=key_schema,
        value_deserializer=lambda x: avro_deserializer(x, value_schema),
        key_deserializer=lambda x: avro_deserializer(x, key_schema)
    )

def read_messages():
    # Update GCS details
    gcs_bucket_name = 'projectcredix_group3_datalake'
    gcs_file_path = 'credit_application/application_record_20231214.csv'
    key_schema_path = "application_record_key.avsc"
    value_schema_path = "application_record_value.avsc"

    key_schema = avro_schema.parse(open(key_schema_path).read())
    value_schema = avro_schema.parse(open(value_schema_path).read())

    consumer = download_avro_records_from_gcs(gcs_bucket_name, gcs_file_path, key_schema, value_schema)
    consumer.subscribe(["practice.application_record_Training"])

    while True:
        try:
            message = consumer.poll(5)
        except Exception as e:
            print(f"Exception while trying to poll messages - {e}")
        else:
            if message is not None:
                print(f"Successfully polled a record from "
                      f"Kafka topic: {message.topic()}, partition: {message.partition()}, offset: {message.offset()}\n"
                      f"message key: {message.key()} || message value: {message.value()}")
                consumer.commit()
                # INSERT STREAM TO BIGQUERY
                client.insert_rows(table, [message.value()])
            else:
                print("No new messages at this point. Try again later.")

    consumer.close()

if __name__ == "__main__":
    read_messages()
