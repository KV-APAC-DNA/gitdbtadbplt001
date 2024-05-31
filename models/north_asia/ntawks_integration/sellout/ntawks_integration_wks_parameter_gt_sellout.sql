
{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                                        DELETE
                    FROM {{this}}
                    WHERE UPPER(DSTR_NM) IN (SELECT DISTINCT DSTR_NM
                                            FROM {{ ref('ntaitg_integration__itg_kr_gt_sellout') }}
                                            WHERE UPPER(DSTR_CD) in ('NH','OTC');
                    {% endif %}"
    )
}}
with itg_kr_gt_sellout as (
    select * from {{ ref('ntaitg_integration__itg_kr_gt_sellout') }}
)
final as (
SELECT DISTINCT 'KR' AS COUNTRY_CD,
       'GT_SELLOUT' AS PARAMETER_NAME,
       dstr_nm
from itg_kr_gt_sellout
WHERE UPPER(DSTR_CD) in ('NH','OTC')
)
select * from final 