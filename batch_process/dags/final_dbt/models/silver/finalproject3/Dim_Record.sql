with transform_dim_record as (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY ID) AS CreditRecordID,
        ID AS Applicant_ID,
        DATE_ADD(DATETIME '2023-10-01 00:00:00', INTERVAL months_balance MONTH) AS Record_Date,
        status,
        score
    FROM {{ ref('cleansing_his') }}
)

Select *
from transform_dim_record
ORDER BY Applicant_ID