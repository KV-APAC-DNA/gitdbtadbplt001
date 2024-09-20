with v_rpt_copa_ciw as(
    select * from {{ ref('aspedw_integration__v_rpt_copa_ciw') }}
),
edw_calendar_dim as(
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
final as
(
               select ctry_nm, cluster, fisc_yr, fisc_yr_per, mega_brand,
                   ciw_desc, 
                   sum(ciw_gts_lcy) as ciw_gts_lcy,
                   sum(ciw_gts_usd) as ciw_gts_usd,
                   sum(ciw_lcy) as ciw_lcy,
                   sum(ciw_usd) as ciw_usd 
         from      
                (select ctry_nm, "cluster" as cluster, fisc_yr, fisc_yr_per, nvl(nullif("b1 mega-brand",''),'NA')  as mega_brand,
                      case when ciw_desc !='Gross Trade Prod Sales' then 'Non_GTS' else 'GTS' end as ciw_desc,
                      case when ciw_desc != 'Gross Trade Prod Sales' then amt_lcy end ciw_lcy ,
                      case when ciw_desc != 'Gross Trade Prod Sales' then amt_usd end ciw_usd ,
                      case when ciw_desc = 'Gross Trade Prod Sales' then amt_lcy  end ciw_gts_lcy,
                      case when ciw_desc = 'Gross Trade Prod Sales' then amt_usd  end ciw_gts_usd
                 from v_rpt_copa_ciw
                where fisc_yr >= (select fisc_yr - 4 from edw_calendar_dim where cal_day = convert_timezone('UTC', current_timestamp())::date)
                and acct_hier_shrt_desc ='NTS'
                )
        group by ctry_nm, cluster,fisc_yr,fisc_yr_per,mega_brand,ciw_desc
)
select * from final