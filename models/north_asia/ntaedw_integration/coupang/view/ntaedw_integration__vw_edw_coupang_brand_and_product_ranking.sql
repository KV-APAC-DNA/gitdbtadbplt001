with 
itg_kr_coupang_brand_ranking as (
select * from {{ ref('ntaitg_integration__itg_kr_coupang_brand_ranking') }}
),
vw_edw_coupang_product_ranking as (
select * from {{ ref('ntaedw_integration__vw_edw_coupang_product_ranking') }}
),
itg_mds_kr_product_ean_mapping as (
select * from {{ ref('ntaitg_integration__itg_mds_kr_product_ean_mapping') }}
),
final as (
SELECT 
  'KR' :: character varying AS ctry_cd, 
  'SOUTH KOREA' :: character varying AS ctry_nm, 
  derived_table1.data_source, 
  (derived_table1.reference_date):: date AS reference_date, 
  derived_table1.data_granularity, 
  derived_table1.category_depth1, 
  derived_table1.category_depth2, 
  derived_table1.category_depth3, 
  derived_table1.ranking, 
  derived_table1.prev_day_ranking, 
  derived_table1.prev_week_ranking, 
  derived_table1.prev_mon_ranking, 
  derived_table1.all_brand, 
  derived_table1.sku_id, 
  derived_table1.sku_name, 
  derived_table1.jnj_brand, 
  derived_table1.rank_change, 
  derived_table1.vendoritemid, 
  derived_table1.ean, 
  derived_table1.jnj_product_flag 
FROM 
  (
    SELECT 
      'brand_ranking' :: character varying AS data_source, 
      to_date(br.yearmo,'YYYYMMDD') AS reference_date, 
      br.data_granularity, 
      br.category_depth1, 
      br.category_depth2, 
      br.category_depth3, 
      br.ranking, 
      NULL :: character varying AS prev_day_ranking, 
      NULL :: character varying AS prev_week_ranking, 
      NULL :: character varying AS prev_mon_ranking, 
      br.brand AS all_brand, 
      '' :: character varying AS sku_id, 
      '' :: character varying AS sku_name, 
      br.jnj_brand, 
      br.rank_change, 
      (NULL :: numeric):: numeric(18, 0) AS vendoritemid, 
      '' :: character varying AS ean, 
      '' :: character varying AS jnj_product_flag 
    FROM 
      itg_kr_coupang_brand_ranking br 
    UNION ALL 
    SELECT 
      'product_ranking' :: character varying AS data_source, 
      to_date(pr.product_ranking_date,'YYYYMMDD') AS reference_date, 
      pr.data_granularity, 
      pr.category_depth1, 
      pr.category_depth2, 
      pr.category_depth3, 
      pr.ranking, 
      pr.prev_day_ranking, 
      pr.prev_week_ranking, 
      pr.prev_mon_ranking, 
      pr.all_brand, 
      pr.coupang_sku_id AS sku_id, 
      pr.coupang_sku_name AS sku_name, 
      '' :: character varying AS jnj_brand, 
      (pr.rank_change):: character varying(5) AS rank_change, 
      ean.vendoritemid, 
      ean.ean, 
      pr.jnj_product_flag 
    FROM 
      (
        vw_edw_coupang_product_ranking pr 
        LEFT JOIN (
          SELECT 
            DISTINCT itg_mds_kr_product_ean_mapping.vendoritemid, 
            itg_mds_kr_product_ean_mapping.skuid, 
            itg_mds_kr_product_ean_mapping.ean 
          FROM 
            itg_mds_kr_product_ean_mapping
        ) ean ON (
          (
            (
              (pr.coupang_sku_id):: numeric
            ):: numeric(18, 0) = ean.skuid
          )
        )
      )
  ) derived_table1
)
select * from final