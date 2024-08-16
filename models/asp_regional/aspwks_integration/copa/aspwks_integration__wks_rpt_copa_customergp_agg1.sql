with wks_rpt_copa_customergp_dim_temp as
(
    select * from {{ ref('aspwks_integration__wks_rpt_copa_customergp_dim_temp') }}
),
wks_rpt_customergp as
(
    select * from {{ ref('aspwks_integration__wks_rpt_customergp') }}
),
final as
(
    select dt.*,custgp.* 
    from wks_rpt_copa_customergp_dim_temp custgp , 
        (
            select distinct 
            fisc_yr as fisc_yr_dt,
            fisc_yr_per as  fisc_yr_per_dt,
            fisc_day    as fisc_day_dt,
            Concat((substring(fisc_yr_per,1,4) - 1),substring(fisc_yr_per,5,7)) AS fisc_previousyr_per_dt
            from wks_rpt_customergp
        ) dt
)
select * from final