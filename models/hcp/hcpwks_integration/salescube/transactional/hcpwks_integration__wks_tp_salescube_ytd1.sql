with wks_tp_salescube_base as
(
    select * from ({{ ref('hcpwks_integration__wks_tp_salescube_base') }})
),
trans1 as
(
    SELECT 
       base.BRAND,
       base.YEAR_MONTH,
       base.REGION,
       base.ZONE,
       base.SALES_AREA,
       -- ,base.SALES_VALUE
       -- ,base.num_buying_retailer
       --,MIN(SUM(YTD.num_buying_retailer_dist_cnt) OVER (PARTITION BY LEFT (base.year_month,4),base.brand,base.region,base.zone,base.sales_area ORDER BY base.year_month ROWS UNBOUNDED PRECEDING)) AS "ret_cnt_ytd"  
       YTD.YEAR_MONTH AS YTD_YEAR_MONTH
    FROM  (
       SELECT BRAND,
              YEAR_MONTH,
              REGION,
              ZONE,
              SALES_AREA
       -- ,SUM(SALES_VALUE) AS SALES_VALUE
       -- ,COUNT(DISTINCT num_buying_retailer) AS num_buying_retailer
       FROM wks_tp_salescube_base
       GROUP BY 1,
              2,
              3,
              4,
              5
       ) base
LEFT JOIN (
       SELECT BRAND,
              YEAR_MONTH,
              REGION,
              ZONE,
              SALES_AREA
       -- ,SUM(SALES_VALUE) AS SALES_VALUE
       -- ,COUNT(DISTINCT num_buying_retailer) AS num_buying_retailer
       FROM wks_tp_salescube_base
       GROUP BY 1,
              2,
              3,
              4,
              5
       ) YTD ON base.BRAND = YTD.BRAND
       AND base.REGION = YTD.REGION
       AND base.ZONE = YTD.ZONE
       AND base.SALES_AREA = YTD.SALES_AREA
       AND TO_DATE(YTD.YEAR_MONTH, 'YYYYMM') >= TO_DATE(LEFT(base.YEAR_MONTH, 4) || '01' || '01', 'YYYYMMDD')
       AND TO_DATE(YTD.YEAR_MONTH, 'YYYYMM') < ADD_MONTHS(TO_DATE(base.YEAR_MONTH, 'YYYYMM'), 1)
),
final as
(
    select
    brand::varchar(20) as brand,
	year_month::varchar(25) as year_month,
	region::varchar(100) as region,
	zone::varchar(50) as zone,
	sales_area::varchar(100) as sales_area,
	ytd_year_month::varchar(25) as ytd_year_month
    from trans1
)
select * from final