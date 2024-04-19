with itg_px_master as
(
  select * from {{ ref('pcfitg_integration__itg_px_master') }}
),
itg_px_weekly_sell as
(
  select * from {{ ref('pcfitg_integration__itg_px_weekly_sell') }}
),
vw_customer_dim as
(
  select * from {{ ref('pcfedw_integration__vw_customer_dim') }}
),
vw_curr_exch_dim as
(
  select * from {{ ref('pcfedw_integration__vw_curr_exch_dim') }}
),
edw_time_dim as
(
  select * from {{ source('pcfedw_integration', 'edw_time_dim') }}
),
source as
(
  SELECT
    a.ac_Code as ac_Code,
    a.ac_longname as ac_longname,
    a.ac_attribute as CUST_ID,
    a.p_promonumber as p_promonumber,
    a.p_startdate as p_startdate,
    a.p_stopdate as p_stopdate,
    a.promo_length as promo_length,
    a.p_buystartdatedef as p_buystartdatedef,
    a.p_buystopdatedef as p_buystopdatedef,
    a.buyperiod_length as buyperiod_length,
    a.hierarchy_rowid as hierarchy_rowid,
    a.hierarchy_longname as hierarchy_longname,
    a.activity_longname as activity_longname,
    a.confirmed_switch as confirmed_switch,
    a.closed_switch as closed_switch,
    a.sku_longname as sku_longname,
    a.sku_stockcode as MATL_ID,
    a.sku_profitcentre as sku_profitcentre,
    a.sku_attribute as sku_attribute,
    a.gltt_rowid as gltt_rowid,
    a.transaction_longname as transaction_longname,
    a.case_deal as case_deal,
    cast(b.case_quantity as integer) as case_quantity,
    b.planspend_total as planspend_total,
    b.paid_total as paid_total,
    a.p_deleted as p_deleted,
    a.transaction_attribute as transaction_attribute,
    b.PromotionForecastWeek as PromotionForecastWeek,
    a.promotionrowid as promotionrowid,
        (
            CASE
                WHEN b.PromotionitemStatus = 'Closed' THEN b.PAID_TOTAL
                ELSE (
                    CASE
                        WHEN b.PLANSPEND_TOTAL > b.PAID_TOTAL THEN b.PLANSPEND_TOTAL
                        ELSE b.PAID_TOTAL
                    END
                )
            END
        ) AS Committed_Spend,
        null as aud_rate,
        null as usd_rate
  from ITG_PX_MASTER a
  join itg_px_weekly_sell b
  on coalesce(a.PromotionRowId, '-999999') = coalesce(b.PromotionRowId, '-999999')
  AND coalesce(a.gltt_rowid, '-999999') = coalesce(b.gltt_rowid, '-999999')
  AND coalesce(a.sku_stockcode, '-999999') = coalesce(b.sku_stockcode, '-999999')
),
vcd as
(
  select distinct cust_no,curr_cd
  from vw_customer_dim vcd,source epmf
  where epmf.cust_id = substring(cust_no, 5, 6)
),
update_1 as
(
  select src.*,
      jj_mnth_id as period,
      vcd.curr_cd as local_ccy
  from source src
  left join edw_time_dim b
  on src.promotionforecastweek = b.cal_date
  left join vcd
  on src.CUST_ID = SUBSTRING(vcd.CUST_NO, 5, 6)
),
nzd as
(
  SELECT
      distinct
      exch_rate,
      cal_date,
      jj_mnth_id,
      from_ccy,
      to_ccy,
      Rate_Type
  FROM vw_curr_exch_dim A
  INNER JOIN
  (
      select
          replace(cal_date::date,'-','') as CAL_DATE,
          jj_mnth_id
      from edw_time_dim
  ) B
  ON A.Valid_Date = B.CAL_DATE,
  update_1 C
  where
      b.JJ_MNTH_ID = C.PERIOD
      AND a.From_CCY = 'NZD'
      AND a.To_CCY = 'AUD'
      AND a.Rate_Type = 'JJBR'
),
vw_curr_filter as
(
  select * from vw_curr_exch_dim
  where rate_type in ('DWBP','JJBR') AND FROM_CCY IN ('AUD','NZD') AND TO_CCY IN ('AUD','USD')
  qualify row_number() over(partition by from_ccy,to_ccy,rate_type order by valid_date desc) = 1
)
,
update_2 as
(
    select 
        a.*,
        case when local_ccy = 'NZD' then b.exch_rate 
        end as aud_rate_nzd,
        case
            when local_ccy = 'AUD' then 1.0
            when local_ccy = 'NZD' and aud_rate_nzd is null then nzd_aud.exch_rate
            else aud_rate_nzd
        end as aud_rate_new,
        case
            when local_ccy = 'NZD' and usd_rate is null then nzd_usd.exch_rate
            when local_ccy = 'AUD' and usd_rate is null then aud_usd.exch_rate
        end as usd_rate_new,
        case when transaction_attribute = '    ' then 'TP'
        else transaction_attribute
        end as transaction_attribute_new
  from
  update_1 a
  left join nzd b
  on a.period = b.jj_mnth_id
  left join vw_curr_filter nzd_aud
  on  nzd_aud.from_ccy = 'NZD' and nzd_aud.to_ccy = 'AUD' and nzd_aud.rate_type = 'JJBR'
  left join vw_curr_filter nzd_usd
  on  nzd_usd.from_ccy = 'NZD' and nzd_usd.to_ccy = 'USD' and nzd_usd.rate_type = 'DWBP'
  left join vw_curr_filter aud_usd
  on  aud_usd.from_ccy = 'AUD' and aud_usd.to_ccy = 'USD' and aud_usd.rate_type = 'DWBP'
),
final as
(
  select 
        ac_code::varchar(50) as ac_code,
        ac_longname::varchar(40) as ac_longname,
        cust_id::varchar(50) as cust_id,
        p_promonumber::varchar(10) as p_promonumber,
        p_startdate::timestamp_ntz(9) as p_startdate,
        p_stopdate::timestamp_ntz(9) as p_stopdate,
        promo_length::number(38,0) as promo_length,
        p_buystartdatedef::timestamp_ntz(9) as p_buystartdatedef,
        p_buystopdatedef::timestamp_ntz(9) as p_buystopdatedef,
        buyperiod_length::number(38,0) as buyperiod_length,
        hierarchy_rowid::number(18,0) as hierarchy_rowid,
        hierarchy_longname::varchar(40) as hierarchy_longname,
        activity_longname::varchar(40) as activity_longname,
        confirmed_switch::number(38,0) as confirmed_switch,
        closed_switch::number(38,0) as closed_switch,
        sku_longname::varchar(40) as sku_longname,
        matl_id::varchar(15) as matl_id,
        sku_profitcentre::varchar(10) as sku_profitcentre,
        sku_attribute::varchar(20) as sku_attribute,
        gltt_rowid::number(18,0) as gltt_rowid,
        transaction_longname::varchar(40) as transaction_longname,
        case_deal::float as case_deal,
        case_quantity::number(18,0) as case_quantity,
        planspend_total::float as planspend_total,
        paid_total::float as paid_total,
        p_deleted::number(38,0) as p_deleted,
        local_ccy::varchar(10) as local_ccy,
        aud_rate_new::float as aud_rate,
        null::float as sgd_rate,
        usd_rate_new::float as usd_rate,
        period::number(18,0) as period,
        transaction_attribute_new::varchar(10) as transaction_attribute,
        promotionforecastweek::timestamp_ntz(9) as promotionforecastweek,
        committed_spend::float as committed_spend,
        promotionrowid::number(18,0 ) as promotionrowid
    from update_2

)
select * from final