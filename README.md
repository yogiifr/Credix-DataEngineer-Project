# Credix: Credit Repayment Ability


[![IYKRA](https://img.shields.io/badge/IYKRA-Data_Fellowship_11-Yellow)](https://www.iykrabatam.com/)
[![Data Engineer](https://img.shields.io/badge/Data-Engineer-green)](https://www.yourdatateacher.com/)

## Overview 🚀
Credix is a project focused on analyzing credit repayment ability. The primary goal is to assess the risk of loan default and take preventive measures to reduce potential issues with credit card users, referred to as debtors.

The project classifies debtors into two categories: good debtors with a positive repayment history and risky debtors with a higher likelihood of default.

The project's outcomes are summarized in three main dashboards: Executive Summary, Debtor Information, and Fraud Monitoring Dashboard.

## Business Process 📊
The analysis of "Credit Repayment Ability" aims to evaluate whether a debtor is good enough in credit repayment, enabling the bank to make informed decisions for future credit transactions. The project addresses the balance between seeking profit through treasury and investment while maintaining financial security with provisions for credit losses.

Default cases impact the bank's risk management, potentially leading to increased provisions and affecting profit due to inadequate risk management and understanding of customers.

## Dataset 📑
Two datasets are utilized in the Credix project:

1. **application_record:** Contains personal information about credit cardholders.
2. **credit_record:** Includes historical credit data of credit cardholders.

A star schema model is employed for the project, with the Fact_CreditCard table serving as the central data hub, connecting numeric and key data. The key references descriptive information available in dimension tables: Dim_Application, Dim_Time, and Dim_Record.

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
### Executive Summary
Provides a concise overview of ongoing credit applicants, including total credit applications, identified good and bad applicants, and summarized characteristics based on income, education, occupation, and gender.

### Debitur Information
Offers detailed information about an applicant, facilitating better decision-making regarding loan approval.

### Fraud Monitoring Dashboard
Aims to identify anomalies and discrepancies to minimize potential risks, such as duplicate IDs with differing information or conflicting employment status and income.

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

## People Behinds Credix
- Yogi Fitriadi Rakhim - Data Engineer
- Rama - Data Engineer
- Ashila - Data Engineer
- Mentari - Data Engineer
