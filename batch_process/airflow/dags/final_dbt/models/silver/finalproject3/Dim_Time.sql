with transform_dim_time as (
    SELECT
        CAST(FORMAT_TIMESTAMP('%Y%m%d', Record_Date) AS INT) AS Time_ID,
        EXTRACT(DAY FROM Record_Date) AS Day,
        EXTRACT(MONTH FROM Record_Date) AS Month,
        EXTRACT(YEAR FROM Record_Date) AS Year,
        Record_Date
    FROM {{ ref('Dim_Record') }}
    group by Record_Date
)

Select *
from transform_dim_time