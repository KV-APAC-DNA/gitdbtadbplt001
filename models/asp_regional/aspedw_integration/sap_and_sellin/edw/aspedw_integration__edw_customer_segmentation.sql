
with source as (
    select * from {{ ref('aspedw_integration__v_rpt_customer_segmentation') }}
),

final as (
  select 
cast("datasource" as varchar(200)) as "datasource",
fisc_yr::number(18,0) as fisc_yr,
fisc_yr_per::number(18,0) as fisc_yr_per,
fisc_day::date as fisc_day,
ctry_nm::varchar(200) as ctry_nm,
co_cd::varchar(4) as co_cd,
company_nm::varchar(100) as company_nm,
sls_org::varchar(500) as sls_org,
sls_org_nm::varchar(20) as sls_org_nm,
dstr_chnl::varchar(2) as dstr_chnl,
dstr_chnl_nm::varchar(20) as dstr_chnl_nm,
cast("cluster" as varchar(200)) as "cluster",
obj_crncy_co_obj::varchar(5) as obj_crncy_co_obj,
cast("parent customer" as varchar(50)) as "parent customer",
banner::varchar(50) as banner,
cast("banner format" as varchar(50)) as "banner format",
channel::varchar(50) as channel,
cast("go to model" as varchar(50)) as "go to model",
cast("sub channel" as varchar(50) ) as "sub channel",
retail_env::varchar(50) as retail_env,
cust_num::varchar(10) as cust_num,
customer_name::varchar(100) as customer_name,
segmt_key::varchar(12) as segmt_key,
segment::varchar(50) as segment,
greenlight_sku_flag::varchar(5) as greenlight_sku_flag,
nts_usd::number(38,15) as nts_usd,
nts_lcy::number(38,15) as nts_lcy,
gts_usd::number(38,15) as gts_usd,
gts_lcy::number(38,15) as gts_lcy,
numerator::number(38,2) as numerator,
denominator::number(38,2) as denominator,
current_timestamp()::timestamp_ntz(9) as crt_dttm
  from source
)


--Final selectv
select * from final 