with edw_calendar_dim as(
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
v_rpt_copa as(
    select * from {{ ref('aspedw_integration__v_rpt_copa') }}
),
final as(
       select distinct cast ('COPA' as varchar(50)) as Datasource, ctry_nm, "cluster" as cluster, fisc_yr_per  , nvl(nullif("parent customer",''),'NA') as parent_customer ,
   nvl(nullif("b1 mega-brand",''),'NA')  as mega_brand, sum(nts_usd) copa_nts_usd , sum(nts_lcy) copa_nts_lcy  
      from v_rpt_copa 
      where fisc_yr >= ( select fisc_yr - 4 from edw_calendar_dim where cal_day = convert_timezone('UTC', current_timestamp())::date)
      group by ctry_nm, "cluster",fisc_yr_per ,"parent customer","b1 mega-brand"
)
select * from final
