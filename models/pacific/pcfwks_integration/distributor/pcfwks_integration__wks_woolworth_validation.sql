with source as(
    select * from {{ source('pcfsdl_raw', 'sdl_dstr_woolworth_inv') }}
),
itg_dstr_woolworth_sap_mapping as(
    select * from {{ ref('pcfitg_integration__itg_dstr_woolworth_sap_mapping') }}
),
final as(
    SELECT
        'WOOLWORTHS' AS dstr_nm,
        inv_date,
        inv.article_code,
        inv.articledesc,
        map.sap_code AS matl_id
    FROM source AS inv
    LEFT JOIN (
    SELECT DISTINCT
        article_code,
        sap_code
    FROM itg_dstr_woolworth_sap_mapping
    WHERE
        sap_code <> ''
    ) AS map
    ON LTRIM(inv.article_code, 0) = LTRIM(map.article_code, 0)
)
select * from final