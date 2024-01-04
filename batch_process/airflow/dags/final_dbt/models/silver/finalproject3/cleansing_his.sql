with clean_his as (
    SELECT  ID,
        MONTHS_BALANCE,
        STATUS,
        CASE 
            when status = 'X' then 0
            when status = 'C' then 1
            when (status != 'X' or status!='C') and status = '0' or status = '1' or status='2' then 2
            when (status != 'X' or status!='C') and status = '3'  then 3
            when (status != 'X' or status!='C') and status = '4'  then 4
            when (status != 'X' or status!='C') and status = '5' then 5
        END as score
    FROM {{ source('projectcredix_group3_bronze', 'raw_credithistory_data') }}

)

Select  ID,
        MONTHS_BALANCE,
        status,
        score
from clean_his
