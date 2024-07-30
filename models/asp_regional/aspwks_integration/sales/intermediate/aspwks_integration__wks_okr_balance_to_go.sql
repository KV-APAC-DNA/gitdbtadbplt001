with wks_rpt_okr_dashboard_intermediate as
(
    select * from dev_dna_core.aspwks_integration.wks_rpt_okr_dashboard_intermediate
),
trans as
(
    SELECT 
      act.year_month,
      act.fisc_year,
      act.quarter,
      act.measure,
      act.brand,
      act.segment,
      act.cluster,
      act.market,
      act.actual,
      tgt.target,
      (tgt.target - act.actual) AS blnc_to_go
    FROM (
      SELECT 
        year_month,
        fisc_year,
        quarter,
        measure,
        brand,
        segment,
        cluster,
        market,
        sum(cy_actual) OVER (
          PARTITION BY fisc_year,
          measure,
          brand,
          segment,
          cluster,
          market ORDER BY year_month ASC rows BETWEEN unbounded preceding
              AND CURRENT row
          ) AS actual
      FROM wks_rpt_okr_dashboard_intermediate
      WHERE year_month IS NOT NULL
        AND measure IN ('NTS', 'GP', 'IBT_NetIncome', 'NTS_from_NPD', 'RGM', 'eCommerce_NTS', 'Free_cash_flow')
      ) act
    LEFT JOIN (
      SELECT fisc_year,
        measure,
        brand,
        segment,
        cluster,
        market,
        sum(bp_target) AS target
      FROM wks_rpt_okr_dashboard_intermediate
      WHERE year_month IS NOT NULL
      GROUP BY 1,
        2,
        3,
        4,
        5,
        6
      ) tgt ON act.fisc_year = tgt.fisc_year
      AND act.measure = tgt.measure
      AND nvl(act.brand, '9999') = nvl(tgt.brand, '9999')
      AND nvl(act.segment, '9999') = nvl(tgt.segment, '9999')
      AND nvl(act.cluster, '9999') = nvl(tgt.cluster, '9999')
      AND nvl(act.market, '9999') = nvl(tgt.market, '9999'
    )
),
final as
(
   select
        year_month::varchar(10) as year_month,
	    fisc_year::varchar(10) as fisc_year,
	    quarter::number(38,0) as quarter,
	    measure::varchar(100) as measure,
	    brand::varchar(200) as brand,
	    segment::varchar(200) as segment,
	    cluster::varchar(100) as cluster,
	    market::varchar(100) as market,
	    actual::number(38,4) as actual,
	    target::number(38,4) as target,
	    blnc_to_go::number(38,4) as blnc_to_go
    from trans
)
select * from final