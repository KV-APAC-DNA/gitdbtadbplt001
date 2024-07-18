--import cte

with ph_re_so as (
    select * from {{ ref('phlitg_integration__itg_ph_re_msl_list_sellout')}}
),
ph_re_pos as (
    select * from {{ ref('phlitg_integration__itg_ph_re_msl_list_pos')}}
),

--final cte

itg_ph_retail_excellence_msl_list as 
(
    select * from ph_re_so
    union
    select * from ph_re_pos
),

final as 
(
    select 
    fisc_yr	:: numeric(18,0) as fisc_yr,
    fisc_per :: numeric(18,0) as fisc_per,
    customer_l0 :: varchar(50) as customer_l0,
    trade_type :: varchar(255) as trade_type,
    sales_group :: varchar(255) as sales_group,
    account_group :: varchar(255) as account_group,
    data_src :: varchar(14) as data_src,
    channel :: varchar(255) as channel,
    sub_channel :: varchar(200) as sub_channel,
    retail_environment :: varchar(382) as retail_environment,
    distributor_code :: varchar(100) as distributor_code,
    distributor_name :: varchar(356) as distributor_name,
    parent_customer :: varchar(75) as parent_customer,
    region :: varchar(255) as region,
    "Area/Zone/Province" :: varchar(255) as "Area/Zone/Province",
    city :: varchar(255) as city,
    sell_out_parent_customer_l2 :: varchar(255) as sell_out_parent_customer_l2,
    sell_out_parent_customer_l1 :: varchar(255) as sell_out_parent_customer_l1,
    sell_out_channel :: varchar(382) as sell_out_channel,
    store_code :: varchar(50) as store_code,
    store_name :: varchar(500) as store_name,
    store_address :: varchar(510) as store_address,
    store_postcode :: varchar(100) as store_postcode,
    store_lat :: varchar(50) as store_lat,
    store_long :: varchar(255) as store_long,
    master_code :: varchar(200) as master_code,
    mapped_sku_cd :: varchar(40) as mapped_sku_cd,
    msl_product_desc :: varchar(300) as msl_product_desc,
    crtd_dttm :: timestamp without time zone as crtd_dttm
from itg_ph_retail_excellence_msl_list
)

--final select

select * from final