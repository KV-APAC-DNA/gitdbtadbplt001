{{
    config
    (
        materialized='incremental',
        incremental_strategy = 'append',
        pre_hook = "
                    {% if is_incremental() %}
                    DELETE
                    FROM {{this}}
                    WHERE (PRODUCT_METRICS_SOURCE_ID) IN (
                        SELECT PRODUCT_METRICS_SOURCE_ID
                        FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_product_metrics') }} STG_PROD
                        WHERE STG_PROD.PRODUCT_METRICS_SOURCE_ID = PRODUCT_METRICS_SOURCE_ID
                        )
                    AND COUNTRY_CODE IN (
                        SELECT DISTINCT COUNTRY_CODE
                        FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_product_metrics') }}
                        );
                    {% endif %}
                    "
    )
}}

WITH sdl_hcp_osea_product_metrics
AS (
  SELECT *
  FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_product_metrics') }}
  ),
itg_product
AS (
  SELECT *
  FROM dev_dna_core.hcposeitg_integration.itg_product
  ),
trns
AS (
  SELECT PRODUCT_METRICS_SOURCE_ID,
    (
      CASE 
        WHEN UPPER(STG_PM.IS_DELETED) = 'TRUE'
          THEN 1
        WHEN UPPER(STG_PM.IS_DELETED) = 'FALSE'
          THEN 0
        ELSE 0
        END
      ) AS IS_DELETED,
    STG_PM.PM_NAME,
    STG_PM.CREATED_DATE,
    STG_PM.CREATED_BY_ID,
    STG_PM.LAST_MODIFIED_DATE,
    STG_PM.LAST_MODIFIED_BY_ID,
    STG_PM.ACCOUNT_SOURCE_ID,
    AWARENESS,
    MOVEMENT,
    STG_PM.PRODUCT_SOURCE_ID,
    SEGMENT,
    X12_MO_TRX_CHG,
    SPEAKER_SKILLS,
    INVESTIGATOR_READINESS,
    ENGAGEMENTS,
    STG_PM.EXTERNAL_ID,
    DECILE,
    ADOPTION_LEVEL,
    DETAIL_GROUP,
    CLASSIFICATION_TYPE,
    COMPANY,
    BELIEVER_OF_ADHERENCE,
    INFLUENCE_LEVEL,
    INTENTION_FOR_FUTURE_SUST,
    LIKELY_TO_INI_SUSTENNA,
    LIKELY_TO_SWITCH,
    PENETRATION,
    POTENTIAL,
    PRESCRIBER,
    NUMBER_OF_PATIENTS_MONTH,
    SCHIZOPHRENIA_PTS,
    SCIENTIFIC_DATA,
    TYPE_OF_SETTING,
    USAGOF_SUSTENNA_UPON_DISCHARGING,
    USAGE_OF_BRANDED_ATYP,
    (
      CASE 
        WHEN UPPER(KOL_EXPERTS) = 'TRUE'
          THEN 1
        WHEN UPPER(KOL_EXPERTS) = 'FALSE'
          THEN 0
        ELSE 0
        END
      ) AS KOL_EXPERTS,
    NBROFMMPTSYR,
    PRACTICE_TYPE,
    STDGUIDELINE,
    PERCEPTION,
    PRESCRIPTION_BEHAVIOR,
    SCIENTIFICALLY_DRIVEN,
    TREATMENT_PATTERN,
    PHYSICIAN_BEHAVIOUR,
    PRODUCT_PREFERENCE,
    SPECIALTY,
    COMPANY_LOYALTY,
    BIOLOGICS_USER,
    PRICE_SENSITIVITY,
    USAGE_OF_GENERIC,
    PREVIOUS_TRAMADOL_EXPERIENCE,
    INTEREST_TO_TREAT_DISEASE_AREA,
    SATISFACTION_WITH_ALTERNATIVE_TR,
    PATIENT_SHARE,
    STG_PM.COUNTRY_CODE,
    PHYSICIAN_PRODUCT_PREFERENCE,
    PHYSICIAN_PRESCRIPTION,
    ADOPTION_LADDER,
    RATINGS_POINTS,
    PEER_INFLUENCE,
    INNOVATIONS,
    CASES_LOADS_PER_YEAR,
    SALES_VALUE_PER_YEAR,
    SUPPORT,
    CMD,
    MD_ASP_CLASSIFICATION,
    NO_OF_PRODUCTS_USED,
    (
      CASE 
        WHEN UPPER(STG_PM.COUNTRY_CODE) = 'ID'
          THEN NULL
        WHEN UPPER(STG_PM.COUNTRY_CODE) = 'VN'
          THEN NULL
        ELSE STG_PM.ORIENTATION_FIELD
        END
      ) AS ORIENTATION_FIELD, -- new column
    (
      CASE 
        WHEN UPPER(STG_PM.COUNTRY_CODE) = 'ID'
          THEN NULL
        WHEN UPPER(STG_PM.COUNTRY_CODE) = 'VN'
          THEN NULL
        ELSE STG_PM.UPTRAVI_USAGE
        END
      ) AS UPTRAVI_USAGE, -- new column
    SYSDATE() AS inserted_date,
    SYSDATE() AS updated_date
  FROM sdl_hcp_osea_product_metrics STG_PM
  JOIN itg_product ITG_P ON ITG_P.PRODUCT_SOURCE_ID = STG_PM.PRODUCT_SOURCE_ID
    AND ITG_P.COUNTRY_CODE = STG_PM.COUNTRY_CODE
    AND ITG_P.PRODUCT_TYPE = 'Detail'
  ),
final
AS (
  SELECT product_metrics_source_id::VARCHAR(18) AS product_metrics_source_id,
    is_deleted::number(38, 0) AS is_deleted,
    pm_name::VARCHAR(80) AS pm_name,
    created_date::timestamp_ntz(9) AS created_date,
    created_by_id::VARCHAR(18) AS created_by_id,
    last_modified_date::timestamp_ntz(9) AS last_modified_date,
    last_modified_by_id::VARCHAR(18) AS last_modified_by_id,
    account_source_id::VARCHAR(18) AS account_source_id,
    awareness::VARCHAR(255) AS awareness,
    movement::number(5, 2) AS movement,
    product_source_id::VARCHAR(18) AS product_source_id,
    segment::VARCHAR(255) AS segment,
    x12_mo_trx_chg::number(5, 2) AS x12_mo_trx_chg,
    speaker_skills::VARCHAR(255) AS speaker_skills,
    investigator_readiness::VARCHAR(255) AS investigator_readiness,
    engagements::number(4, 0) AS engagements,
    external_id::VARCHAR(255) AS external_id,
    decile::number(18, 0) AS decile,
    adoption_level::VARCHAR(255) AS adoption_level,
    detail_group::VARCHAR(18) AS detail_group,
    classification_type::VARCHAR(255) AS classification_type,
    company::number(18, 0) AS company,
    believer_of_adherence::VARCHAR(255) AS believer_of_adherence,
    influence_level::VARCHAR(255) AS influence_level,
    intention_for_future_sust::VARCHAR(255) AS intention_for_future_sust,
    likely_to_ini_sustenna::VARCHAR(255) AS likely_to_ini_sustenna,
    likely_to_switch::VARCHAR(255) AS likely_to_switch,
    penetration::number(3, 0) AS penetration,
    potential::number(3, 0) AS potential,
    prescriber::VARCHAR(255) AS prescriber,
    number_of_patients_month::number(3, 0) AS number_of_patients_month,
    schizophrenia_pts::number(6, 0) AS schizophrenia_pts,
    scientific_data::VARCHAR(255) AS scientific_data,
    type_of_setting::VARCHAR(255) AS type_of_setting,
    usagof_sustenna_upon_discharging::VARCHAR(255) AS usagof_sustenna_upon_discharging,
    usage_of_branded_atyp::VARCHAR(255) AS usage_of_branded_atyp,
    kol_experts::number(38, 0) AS kol_experts,
    nbrofmmptsyr::number(10, 0) AS nbrofmmptsyr,
    practice_type::VARCHAR(255) AS practice_type,
    stdguideline::VARCHAR(255) AS stdguideline,
    perception::VARCHAR(255) AS perception,
    prescription_behavior::VARCHAR(255) AS prescription_behavior,
    scientifically_driven::VARCHAR(255) AS scientifically_driven,
    treatment_pattern::VARCHAR(255) AS treatment_pattern,
    physician_behaviour::VARCHAR(255) AS physician_behaviour,
    product_preference::VARCHAR(255) AS product_preference,
    specialty::VARCHAR(255) AS specialty,
    company_loyalty::VARCHAR(255) AS company_loyalty,
    biologics_user::VARCHAR(255) AS biologics_user,
    price_sensitivity::VARCHAR(255) AS price_sensitivity,
    usage_of_generic::VARCHAR(255) AS usage_of_generic,
    previous_tramadol_experience::VARCHAR(255) AS previous_tramadol_experience,
    interest_to_treat_disease_area::VARCHAR(255) AS interest_to_treat_disease_area,
    satisfaction_with_alternative_tr::VARCHAR(255) AS satisfaction_with_alternative_tr,
    patient_share::VARCHAR(255) AS patient_share,
    country_code::VARCHAR(1300) AS country_code,
    physician_product_preference::VARCHAR(255) AS physician_product_preference,
    physician_prescription::VARCHAR(255) AS physician_prescription,
    adoption_ladder::VARCHAR(255) AS adoption_ladder,
    ratings_points::VARCHAR(255) AS ratings_points,
    peer_influence::VARCHAR(255) AS peer_influence,
    innovations::VARCHAR(255) AS innovations,
    cases_loads_per_year::VARCHAR(255) AS cases_loads_per_year,
    sales_value_per_year::VARCHAR(255) AS sales_value_per_year,
    support::VARCHAR(255) AS support,
    cmd::VARCHAR(255) AS cmd,
    md_asp_classification::VARCHAR(255) AS md_asp_classification,
    no_of_products_used::VARCHAR(255) AS no_of_products_used,
    orientation_field::VARCHAR(255) AS orientation_field,
    uptravi_usage::VARCHAR(255) AS uptravi_usage,
    inserted_date::timestamp_ntz(9) AS inserted_date,
    updated_date::timestamp_ntz(9) AS updated_date
  FROM trns
  )
SELECT *
FROM final
