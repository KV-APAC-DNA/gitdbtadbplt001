with source as
(
    select * from indedw_integration.edw_rpt_sales_details
    --{{ ref('indedw_integration__edw_rpt_sales_details') }}
),
final as
(
    SELECT sf.mth_mm,
       sf.week,
       sf.customer_code,
       sf.retailer_code,
       sf.mothersku_code,
       SUM(sf.quantity) AS quantity,
       SUM(sf.achievement_nr) AS achievement_nr_val,
       COUNT(DISTINCT sf.num_lines) AS num_lines,
       COUNT(DISTINCT sf.product_code) AS num_packs,
       TO_DATE((sf.mth_mm)::CHARACTER VARYING::TEXT,'YYYYMM'::CHARACTER VARYING::TEXT) AS sales_date
    FROM source sf
    WHERE mth_mm >= 202111                    -- TBC: paremterize this
    GROUP BY sf.mth_mm,
            sf.week,
            sf.customer_code,
            sf.retailer_code,
            sf.mothersku_code,
            TO_DATE((sf.mth_mm)::CHARACTER VARYING::TEXT,'YYYYMM'::CHARACTER VARYING::TEXT)
)

select * from final