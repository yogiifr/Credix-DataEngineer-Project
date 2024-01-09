with transform_dim_fact as (
    SELECT app.Applicant_ID as APPLICANT_ID,
        his.CreditRecordID,
        his.score,
        case when score < 3 then 1
            else 0 
            end as Good_Debt,
        case when score > 2 then 1
            else 0 
            end as Bad_Debt,
        timee.Time_ID
    FROM {{ ref('Dim_Application') }} app
    LEFT JOIN {{ ref('Dim_Record') }} his ON app.Applicant_ID = his.Applicant_ID
    LEFT JOIN {{ ref('Dim_Time') }} timee ON his.Record_Date = timee.Record_Date
)

Select *
from transform_dim_fact