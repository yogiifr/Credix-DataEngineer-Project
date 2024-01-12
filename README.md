# Credix: Credit Repayment Ability


[![IYKRA](https://img.shields.io/badge/IYKRA-Data_Fellowship_11-Yellow)](https://www.iykrabatam.com/)
[![Data Engineer](https://img.shields.io/badge/Data-Engineer-green)](https://www.yourdatateacher.com/)
[![Pitch Deck](https://img.shields.io/badge/Pitch_Deck-Here-blue)](https://docs.google.com/presentation/d/1RbEj9m23d0VJ9U1Yuz-poZXN0hWVtlM4pv5kLJkZC9Y/edit?usp=sharing)


## Overview 🚀
Credix is a project focused on analyzing credit repayment ability. The primary goal is to assess the risk of loan default and take preventive measures to reduce potential issues with credit card users, referred to as debtors.

The project classifies debtors into two categories: good debtors with a positive repayment history and risky debtors with a higher likelihood of default.

The project's outcomes are summarized in three main dashboards: Executive Summary, Debtor Information, and Fraud Monitoring Dashboard.

## Business Process 📊
The analysis of "Credit Repayment Ability" aims to evaluate whether a debtor is good enough in credit repayment, enabling the bank to make informed decisions for future credit transactions. The project addresses the balance between seeking profit through treasury and investment while maintaining financial security with provisions for credit losses.

Default cases impact the bank's risk management, potentially leading to increased provisions and affecting profit due to inadequate risk management and understanding of customers.

## Dataset 📑
![Dataset](images/dataset.png)
Two datasets are utilized in the Credix project:

1. **application_record:** Contains personal information about credit cardholders.
2. **credit_record:** Includes historical credit data of credit cardholders.

A star schema model is employed for the project, with the Fact_CreditCard table serving as the central data hub, connecting numeric and key data. The key references descriptive information available in dimension tables: Dim_Application, Dim_Time, and Dim_Record.

## Data Processing ⚙️
**Application_Record**
1. Days_Birth -> Year_Birth
2. Days_Employed -> Year_Employed
3. Bagian Flag sebelumnya Y&N-> ditreat jadi 0 1 untuk boolean
4. Bagian days employed untuk tipe data debitur yang pensiun, awalnya data memiliki value 365 ribu hari atau 1000 tahun, kami proses menjadi default 50 tahun atau sekitar 18.250 hari
5. Untuk kolom occupation type beberapa id memiliki tipe ocupation yang null sehingga kami proses menjadi other
**Credit_Record**
1. Pada awalnya kolom month balance pada credit record itu memiliki value range waktu yang belum terlalu terdefinisi, pada kasus ini kami memproses dan men set default waktu dari rentang per tanggal 1 oktober 2023
2. Untuk kolom status kami menggunakan credit skoring berdasarkan bi checking
  Skor 0 : Tidak melakukan kredit
  Skor 1: Kredit Lancar, artinya debitur selalu memenuhi kewajibannya untuk membayar cicilan setiap bulan beserta bunganya hingga lunas tanpa pernah menunggak. 
  Skor 2: Kredit dalam Perhatian Khusus, artinya debitur tercatat menunggak cicilan kredit 1-90 hari 
  Skor 3: Kredit Tidak Lancar, artinya debitur tercatat menunggak cicilan kredit 91-120 hari 
  Skor 4: Kredit Diragukan, artinya debitur tercatat menunggak cicilan kredit 121-180 hari 
  Skor 5: Kredit Macet, artinya debitur tercatat menunggak cicilan kredit lebih 180 hari.

## Tech Stack 💻
The project employs various tools and technologies:

- [**Infrastructure as Code:** Terraform](https://github.com/yogiifr/Credix-DataEngineer-Project/tree/main/infrastructure_provisioning/terraform)
- **Cloud Platform:** Google Cloud Platform (GCP)
- **Data Storage:** Google Cloud Storage
- **Data Warehouse:** BigQuery
- [**Streaming Processing Tools:** Kafka ](https://github.com/yogiifr/Credix-DataEngineer-Project/tree/main/stream_process/kafka)
- [**Batch Processing Tools:** Airflow ](https://github.com/yogiifr/Credix-DataEngineer-Project/tree/main/batch_process/airflow)
- [**ETL/ELT:** dbt](https://github.com/yogiifr/Credix-DataEngineer-Project/tree/main/batch_process/airflow)
- [**Data Serialization:** Avro](https://github.com/yogiifr/Credix-DataEngineer-Project/tree/main/stream_process/kafka)
- **Containerization:** Docker
- [**Visualization:** Tableau](https://github.com/yogiifr/Credix-DataEngineer-Project/tree/main/dashboard)

## Data Pipeline  🛠️
![Architecture](images/architecture.png)
The end-to-end pipeline architecture involves the following steps:

1. Local data is pulled using Airflow and Docker, stored in Google Cloud Storage (Data Lake).
2. Kafka is used for data streaming to transfer data to BigQuery, utilizing Docker and Avro for data optimization.
3. Terraform manages the infrastructure as code, feeding into the Data Warehouse.
4. Data in the Data Warehouse undergoes processing into raw (bronze), silver, and gold layers.
5. The gold layer results in Executive Summary and Credit Card Applicant insights, visualized in dashboards.

## Cost Estimation 💸

**1GB Monthly Case**    
Total Estimated Cost: $0.04 / Rp. 621,12

**50GB Monthly Case**   
Total Estimated Cost: $4.31 / Rp. 66.904

In this project, we recommend cost-saving strategies:

- Use Cost Management Tools: Leverage GCP tools for monitoring and cost control.
- Optimize VM and DataProc Sizes: Adjust sizes based on actual needs.

- Consider Preemptible VMs: Explore lower-cost temporary instances.

- Implement Auto Scaling for Efficiency: Set up auto-scaling for dynamic resource adjustment.

- Optimize Storage Costs: Use storage lifecycle management to transition data effectively.

- Choose Region and Storage Class Wisely: Align choices with project needs for a balance of performance and cost.
 
## Dashboard 📈
![dashboard_title](images/dashboard_title.gif)
### Executive Summary
Provides a concise overview of ongoing credit applicants, including total credit applications, identified good and bad applicants, and summarized characteristics based on income, education, occupation, and gender.
![Executive_Summary](images/executive_summary.png)

### Debitur Information
Offers detailed information about an applicant, facilitating better decision-making regarding loan approval.
![Debitur_Information](images/debitur_information.png)

### Fraud Monitoring Dashboard
Aims to identify anomalies and discrepancies to minimize potential risks, such as duplicate IDs with differing information or conflicting employment status and income.
![Fraud_Monitoring](images/fraud_monitoring.png)

## Conclusions & Improvement 🚧
**Conclusion:**
- Completed an end-to-end data pipeline.
- Achieved objectives of the three dashboards.
- Connected to the bank's risk prevention measures.
- Timely analysis to minimize debtor risks.

**Improvement:**
- Implement Machine Learning for enhanced risk prediction.
- Automate early warning signals.
- Develop more proactive strategies using predictive analytics for timely interventions.

## How To Run This Project?
### Clone this repository and enter the directory
```bash
git clone https://github.com/yogiifr/Credix-DataEngineer-Project.git && cd Credix-DataEngineer-Project
```


### Create a file named "service-account.json" containing your Google service account credentials and copy file to dbt folder
```json
{
  "type": "service_account",
  "project_id": "[PROJECT_ID]",
  "private_key_id": "[KEY_ID]",
  "private_key": "-----BEGIN PRIVATE KEY-----\n[PRIVATE_KEY]\n-----END PRIVATE KEY-----\n",
  "client_email": "[SERVICE_ACCOUNT_EMAIL]",
  "client_id": "[CLIENT_ID]",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://accounts.google.com/o/oauth2/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/[SERVICE_ACCOUNT_EMAIL]"
}
```
### Infra Provisioning

1. Install `gcloud` SDK, `terraform` CLI, and create a GCP project. Then, create a service account with **Storage Admin**, **Storage Pbject Admin**, and **BigQuery Admin** role. Download the JSON credential and store it on `service-account.json`. Open `terraform/main.tf` in a text editor, and fill your GCP's project id.

2. Enable IAM API and IAM Credential API in GCP.
:w
uu
3. Change directory to `terraform` by executing
```
cd terraform
```

4. Initialize Terraform (set up environment and install Google provider)
```
terraform init
```
5. Plan Terraform infrastructure creation
```
terraform plan
```
6. Create new infrastructure by applying Terraform plan
```
terraform apply
```
7. Check GCP console to see newly-created resources.

### Batch Processing

1. Setting dbt in profiles.yml

2. Create batch pipeline with Docker Compose
```bash
sudo docker-compose up
```
3. Open Airflow with username and password "airflow" to run the DAG
```
localhost:8080
```
![dags](images/dags.png)

### Streaming Processing

1. Enter directory kafka
```bash
cd kafka
```

2. Create streaming pipeline with Docker Compose
```bash
sudo docker-compose up
```

3. Install required Python packages
```bash
pip install -r requirements.txt
```

4. Run the producer to stream the data into the Kafka topic
```bash
python producer.py
```

5. Run the consumer to consume the data from Kafka topic and load them into BigQuery
```bash
python consumer.py
```

![Screenshot (190)](https://user-images.githubusercontent.com/108534539/230141794-eb04880c-bf5e-4566-aa94-cbe8501e6e3f.png)

6. Open Confluent to view the topic
```
localhost:9021
```
![confluent](images/confluent.png)

7. Open Schema Registry to view the active schemas
```
localhost:8081/schemas
```
![consume_produce](images/consume_produce.jpeg)

## People Behinds Credix
- Yogi Fitriadi Rakhim - Data Engineer
- Rama - Data Engineer
- Ashila - Data Engineer
- Mentari - Data Engineer
