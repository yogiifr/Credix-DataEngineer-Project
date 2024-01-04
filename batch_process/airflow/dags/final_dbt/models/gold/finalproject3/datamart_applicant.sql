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

, data_his_cust as (
  select Applicant_ID,
    count(*) as total_record,
    sum(good_debt) as good_debt,
    sum(bad_debt) as bad_debt,
    sum(frekuensi) as frekuensi,
    sum(due) as due,
    sum(fluent) as fluent
  from count_data
  group by Applicant_ID
)

, join_all as (
select A.*,
    x.total_record,
    x.good_debt,
    x.bad_debt,
    x.frekuensi,
    x.due,
    x.fluent
from data_his_cust x
left join  {{ ref('Dim_Application') }} A on x.Applicant_ID = A.Applicant_ID
)

select*
from join_all
       
