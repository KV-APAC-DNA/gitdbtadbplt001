with edw_sku_recom_spike_msl as
(
    select * from snapindedw_integration.edw_sku_recom_spike_msl
),
final as 
(
    SELECT mth_mm,
       cust_cd,
       retailer_cd,
       'YES' AS "os_y/n_flag"
      FROM (SELECT mth_mm,
                   cust_cd,
                   retailer_cd,
                   salesman_code,
                   SUM(COALESCE(A3,0) *1.0) /NULLIF(SUM(COALESCE(A1,0)),0) AS DIVI,
                   SUM(COALESCE(A1,0)) AS recos,
                   SUM(COALESCE(A3,0)) AS hits
            FROM (SELECT cust_cd,
                         salesman_code,
                         retailer_cd,
                         mother_sku_cd,
                         mth_mm,
                         MAX(ms_flag)::NUMERIC AS A1,
                         MAX(hit_ms_flag)::NUMERIC AS A3
                  FROM edw_sku_recom_spike_msl a
                  WHERE LEFT(mth_mm,4) >= 2022
                  GROUP BY 1,2,3,4,5) base_tbl
            GROUP BY 1,2,3,4) iNN
      WHERE DIVI >= 0.50
      GROUP BY 1,2,3
)
select * from final