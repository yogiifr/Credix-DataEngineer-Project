with count_data as (
  SELECT A.*,
    case when A.score = 0 then 0
    when A.score >0 and A.score <=5 then 1 end as frekuensi,

    case when A.score > 1 then 1
    when A.score = 1  then 0 end as due,

    case when A.score = 1 then 1
    when  A.score > 1 then 0 end as fluent

  FROM  {{ ref('Dim_Fact') }}  A
) 



, join_all as (
select A.*,
    B.Year,
    B.Month,
    x.good_debt,
    x.bad_debt,
    x.frekuensi,
    x.due,
    x.fluent
from count_data x
left join  {{ ref('Dim_Application') }} A on x.Applicant_ID = A.Applicant_ID
left join  {{ ref('Dim_Time') }} B on x.Time_ID = B.Time_ID
)

select*
from join_all
order by Applicant_ID
       
