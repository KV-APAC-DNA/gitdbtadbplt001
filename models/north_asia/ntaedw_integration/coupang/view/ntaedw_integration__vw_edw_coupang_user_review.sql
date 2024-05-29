with itg_kr_coupang_daily_brand_reviews as (
select * from DEV_DNA_CORE.NTAITG_INTEGRATION.ITG_KR_COUPANG_DAILY_BRAND_REVIEWS
),
itg_mds_kr_product_ean_mapping as (
select * from DEV_DNA_CORE.NTAITG_INTEGRATION.ITG_MDS_KR_PRODUCT_EAN_MAPPING
),
final as (
SELECT 
  'KR' :: character varying AS ctry_cd, 
  'SOUTH KOREA' :: character varying AS ctry_nm, 
  'user_review' :: character varying AS data_source, 
  (rev.review_date):: date AS reference_date, 
  rev.data_granularity, 
  rev.brand AS all_brand, 
  (
    (
      (rev.coupang_id):: numeric
    ):: numeric(18, 0)
  ):: numeric(31, 0) AS vendoritemid, 
  rev.coupang_product_name, 
  rev.review_score_star, 
  rev.review_contents, 
  (ean.skuid):: character varying(15) AS sku_id, 
  ean.ean 
FROM 
  (
   itg_kr_coupang_daily_brand_reviews rev 
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
          (rev.coupang_id):: numeric
        ):: numeric(18, 0) = ean.vendoritemid
      )
    )
  )
)
select * from final
