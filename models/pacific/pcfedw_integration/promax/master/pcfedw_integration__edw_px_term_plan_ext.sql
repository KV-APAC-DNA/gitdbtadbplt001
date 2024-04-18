with edw_px_term_plan as(
    select * from {{ ref('pcfedw_integration__edw_px_term_plan') }}
),
cte1 as(
    SELECT
        edw_px_term_plan.ac_code,
        edw_px_term_plan.ac_attribute,
        edw_px_term_plan.ac_longname,
        edw_px_term_plan.sku_stockcode,
        edw_px_term_plan.sku_attribute,
        edw_px_term_plan.sku_longname,
        edw_px_term_plan.gltt_longname,
        edw_px_term_plan.bd_shortname,
        edw_px_term_plan.bd_longname,
        edw_px_term_plan.asps_type,
        SUM(edw_px_term_plan.asps_month1) AS asps_month1,
        SUM(edw_px_term_plan.asps_month2) AS asps_month2,
        SUM(edw_px_term_plan.asps_month3) AS asps_month3,
        SUM(edw_px_term_plan.asps_month4) AS asps_month4,
        SUM(edw_px_term_plan.asps_month5) AS asps_month5,
        SUM(edw_px_term_plan.asps_month6) AS asps_month6,
        SUM(edw_px_term_plan.asps_month7) AS asps_month7,
        SUM(edw_px_term_plan.asps_month8) AS asps_month8,
        SUM(edw_px_term_plan.asps_month9) AS asps_month9,
        SUM(edw_px_term_plan.asps_month10) AS asps_month10,
        SUM(edw_px_term_plan.asps_month11) AS asps_month11,
        SUM(edw_px_term_plan.asps_month12) AS asps_month12,
        edw_px_term_plan.gltt_rowid
    FROM edw_px_term_plan
    GROUP BY
        edw_px_term_plan.ac_code,
        edw_px_term_plan.ac_attribute,
        edw_px_term_plan.ac_longname,
        edw_px_term_plan.sku_stockcode,
        edw_px_term_plan.sku_attribute,
        edw_px_term_plan.sku_longname,
        edw_px_term_plan.gltt_longname,
        edw_px_term_plan.bd_shortname,
        edw_px_term_plan.bd_longname,
        edw_px_term_plan.asps_type,
        edw_px_term_plan.gltt_rowid
),
transformed as(
    SELECT
        '0000'::varchar(10) as cmp_id,
        '00'::varchar(20) as channel_id,,
        ac_attribute::varchar(50) as cust_id,
        sku_stockcode::varchar(15) as matl_id,
        gltt_longname::varchar(40) as gltt_longname,
       -- timeperiod::number(18,0) as time_period,
        case
            when time_period='1' then TO_NUMERIC(year || '01')
            when time_period='2' then TO_NUMERIC(year ||'02')
            when time_period='3' then TO_NUMERIC(year ||'03')
            when time_period='4' then TO_NUMERIC(year ||'04')
            when time_period='5' then TO_NUMERIC(year ||'05')
            when time_period='6' then TO_NUMERIC(year ||'06')
            when time_period='7' then TO_NUMERIC(year ||'07')
            when time_period='8' then TO_NUMERIC(year ||'08')
            when time_period='9' then TO_NUMERIC(year ||'09')
            when time_period='10' then TO_NUMERIC(year ||'10')
            when time_period='11' then TO_NUMERIC(year ||'11')
            when time_period='12' then TO_NUMERIC(year ||'12')
        else time_period
        end::number(18,0) as time_period,
        CASE timeperiod
            WHEN 1
            THEN asps_month1
            WHEN 2
            THEN asps_month2
            WHEN 3
            THEN asps_month3
            WHEN 4
            THEN asps_month4
            WHEN 5
            THEN asps_month5
            WHEN 6
            THEN asps_month6
            WHEN 7
            THEN asps_month7
            WHEN 8
            THEN asps_month8
            WHEN 9
            THEN asps_month9
            WHEN 10
            THEN asps_month10
            WHEN 11
            THEN asps_month11
            WHEN 12
            THEN asps_month12
        END::float AS px_term_proj_amt,
        bd_shortname::varchar(4) as year,
        gltt_rowid::number(18,0) as gltt_rowid
FROM cte1, (
  SELECT
    1 AS timeperiod
  UNION ALL
  SELECT
    2
  UNION ALL
  SELECT
    3
  UNION ALL
  SELECT
    4
  UNION ALL
  SELECT
    5
  UNION ALL
  SELECT
    6
  UNION ALL
  SELECT
    7
  UNION ALL
  SELECT
    8
  UNION ALL
  SELECT
    9
  UNION ALL
  SELECT
    10
  UNION ALL
  SELECT
    11
  UNION ALL
  SELECT
    12
) AS m
WHERE
  ac_attribute <> '  '
),
final as(
    select 
        cmp_id,
        --channel_id,
        case
         when channel_id = '10' and cmp_id = '7470' then channel_id = '10 - au'
         when channel_id = '11' and cmp_id = '7470' then channel_id = '11 - au'
         when channel_id = '15' and cmp_id = '7470' then channel_id = '15 - au'
         when channel_id = '19' and cmp_id = '7470' then channel_id = '19 - au'
         when channel_id = '10' and cmp_id = '8361' then channel_id = '10 - nz'
         when channel_id = '11' and cmp_id = '8361' then channel_id = '11 - nz'
        else channel_id
        end as channel_id
        cust_id,
        matl_id,
        gltt_longname,
        time_period,
        px_term_proj_amt,
        year,
        gltt_rowid,
    from transformed
)
select * from transformed