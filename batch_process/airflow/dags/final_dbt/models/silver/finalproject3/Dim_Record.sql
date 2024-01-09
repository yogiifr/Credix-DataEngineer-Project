with transform_dim_record as (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY ID) AS RECORD_ID,
        ID AS APPLICANT_ID,
        DATE_ADD(DATETIME '2023-10-01 00:00:00', INTERVAL months_balance MONTH) AS RECORD_DATE,
        status,
        score
    FROM {{ ref('cleansing_his') }}
)

Select *
from transform_dim_record
ORDER BY APPLICANT_ID