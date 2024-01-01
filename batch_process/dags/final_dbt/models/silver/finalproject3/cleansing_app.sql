WITH clean_app AS (
    SELECT  ID,
            CODE_GENDER,
            {{ years_birth(days_birth) }} as YEARS_BIRTH,
            {{ fill_null(occupation_type) }} as OCCUPATION_TYPE,
            {{ years_employed(days_employed) }} as YEARS_EMPLOYED,
            NAME_INCOME_TYPE,
            AMT_INCOME_TOTAL,
            NAME_EDUCATION_TYPE,
            NAME_FAMILY_STATUS,
            CNT_FAM_MEMBERS,
            CNT_CHILDREN,
            NAME_HOUSING_TYPE,
            {{ change_to_int('flag_own_car') }} as FLAG_OWN_CAR,
            {{ change_to_int('flag_own_realty') }} as FLAG_OWN_REALTY,
            FLAG_MOBIL,
            FLAG_WORK_PHONE,
            FLAG_PHONE,
            FLAG_EMAIL,
            COUNT(*) OVER (PARTITION BY ID) as id_count
    FROM {{ source('projectcredix_group3_bronze', 'raw_applicationrecord_data') }}
)

SELECT  ID,
        CODE_GENDER,
        CAST(YEARS_BIRTH as int) as YEARS_BIRTH,
        OCCUPATION_TYPE,
        CAST(YEARS_EMPLOYED as int) as YEARS_EMPLOYED,
        NAME_INCOME_TYPE,
        AMT_INCOME_TOTAL,
        NAME_EDUCATION_TYPE,
        NAME_FAMILY_STATUS,
        CAST(CNT_FAM_MEMBERS as int) as CNT_FAM_MEMBERS,
        CNT_CHILDREN,
        NAME_HOUSING_TYPE,
        FLAG_OWN_CAR,
        FLAG_OWN_REALTY,
        FLAG_MOBIL,
        FLAG_WORK_PHONE,
        FLAG_PHONE,
        FLAG_EMAIL
FROM clean_app
-- Drop Duplicate ID
WHERE id_count = 1