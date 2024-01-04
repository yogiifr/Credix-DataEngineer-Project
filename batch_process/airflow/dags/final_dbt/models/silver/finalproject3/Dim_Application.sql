with transform_dim_app as (
    SELECT 
            ID AS Applicant_ID,
            CODE_GENDER,
            YEARS_BIRTH,
            OCCUPATION_TYPE,
            YEARS_EMPLOYED,
            NAME_INCOME_TYPE,
            AMT_INCOME_TOTAL,
            NAME_EDUCATION_TYPE,
            NAME_FAMILY_STATUS,
            CNT_FAM_MEMBERS,
            CNT_CHILDREN,
            NAME_HOUSING_TYPE,
            {{ change_to_string('flag_own_car') }} as FLAG_OWN_CAR,
            {{ change_to_string('flag_own_realty') }} as FLAG_OWN_REALTY,
            {{ change_to_string('flag_mobil') }} as FLAG_MOBIL,
            {{ change_to_string('flag_work_phone') }} as FLAG_WORK_PHONE,
            {{ change_to_string('flag_phone') }} as FLAG_PHONE,
            {{ change_to_string('flag_email') }} as FLAG_EMAIL,
    FROM {{ ref('cleansing_app') }}
)

Select *
from transform_dim_app