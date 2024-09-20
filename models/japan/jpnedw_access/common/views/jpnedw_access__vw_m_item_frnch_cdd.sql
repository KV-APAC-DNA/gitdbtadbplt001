with source as(
    select * from {{ ref('jpnedw_integration__vw_m_item_frnch_cdd') }}
),
transformed as(
    SELECT 
    item_cd as "item_cd",
    frnch_group_cd as "frnch_group_cd",
    frnch_group_nm as "frnch_group_nm",
    frnch_group_srt as "frnch_group_srt",
    frnch_cd as "frnch_cd",
    frnch_nm as "frnch_nm",
    frnch_srt as "frnch_srt",
    mjr_prod_cd as "mjr_prod_cd",
    mjr_prod_nm as "mjr_prod_nm",
    mjr_prod_srt as "mjr_prod_srt",
    mjr_prod_cd2 as "mjr_prod_cd2",
    mjr_prod_nm2 as "mjr_prod_nm2",
    mjr_prod_srt2 as "mjr_prod_srt2",
    min_prod_cd as "min_prod_cd",
    min_prod_nm as "min_prod_nm",
    min_prod_srt as "min_prod_srt"
    FROM source
)
select * from transformed