with wks_rev_edw_dailysales_fact as
(
    select * from {{ ref('indwks_integration__wks_rev_edw_dailysales_fact') }}
),
final as
(
SELECT sf.mth_mm,
       sf.customer_code,
       sf.retailer_code,
       SUM(sf.achievement_nr_val) AS achievement_nr_val_br,
       TO_DATE((sf.mth_mm)::CHARACTER VARYING::TEXT||'01','YYYYMMDD'::CHARACTER VARYING::TEXT) AS sales_date
FROM wks_rev_edw_dailysales_fact sf
GROUP BY sf.mth_mm,
         sf.customer_code,
         sf.retailer_code,
         TO_DATE((sf.mth_mm)::CHARACTER VARYING::TEXT||'01','YYYYMMDD'::CHARACTER VARYING::TEXT)
)
select * from final
