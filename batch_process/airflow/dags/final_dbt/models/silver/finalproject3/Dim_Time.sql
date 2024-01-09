with transform_dim_time as (
    SELECT
        CAST(FORMAT_TIMESTAMP('%Y%m%d', RECORD_DATE) AS INT) AS Time_ID,
        EXTRACT(DAY FROM RECORD_DATE) AS Day,
        EXTRACT(MONTH FROM RECORD_DATE) AS Month,h,
        EXTRACT(YEAR FROM RECORD_DATE) AS Year,
        RECORD_DATE
    FROM {{ ref('Dim_Record') }}
    group by RECORD_DATE
)

Select *
from transform_dim_time