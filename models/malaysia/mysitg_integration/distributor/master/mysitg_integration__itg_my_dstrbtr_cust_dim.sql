{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['outlet_key']
    )
}}

with source as (
    select * from {{ ref('mysitg_integration__itg_my_outlet_attr') }}
),

itg_my_sellout_sales_fact as (
    select * from {{ ref('mysitg_integration__itg_my_sellout_sales_fact') }}
),

union_1 as (
    select distinct
  a.dstrbtr_id || a.cust_cd as outlet_key,
  a.dstrbtr_id as cust_id,
  'Not-Known' as cust_nm,
  a.cust_cd as outlet_id,
  'Not-Known' as outlet_desc,
  'Not-Known' as outlet_type1,
  'Not-Known' as outlet_type2,
  'Not-Known' as outlet_type3,
  'Not-Known' as outlet_type4,
  'Not-Known' as town,
  'Not-Known' as cust_year,
  'Not-Known' as slsmn_cd,
  current_timestamp() as crtd_dttm,
  cast(null as date) as updt_dttm
from itg_my_sellout_sales_fact as a
{% if is_incremental() %}
    where outlet_key not in( select distinct outlet_key from {{ this }} )
{% endif %}
),

union_2 as (
select
  upper(trim(cust_id || outlet_id)) as outlet_key, 
  cust_id as cust_id, 
  cust_nm as cust_nm, 
  outlet_id as outlet_id, 
  outlet_desc as outlet_desc, 
  outlet_type1 as outlet_type1, 
  outlet_type2 as outlet_type2, 
  outlet_type3 as outlet_type3, 
  outlet_type4 as outlet_type4, 
  town as town, 
  cust_year as cust_year, 
  slsmn_cd as slsmn_cd, 
  current_timestamp() as crtd_dttm, 
  null as updt_dttm 
from source
),

union_final as
(
    (
        select * from union_1 
        where outlet_key not in (select distinct outlet_key from union_2)
    )
union all
    (
        select * from union_2 
    )

),

final as 
( 
    select 
        outlet_key::varchar(100) as outlet_key,
        cust_id::varchar(20) as cust_id,
        cust_nm::varchar(100) as cust_nm,
        outlet_id::varchar(50) as outlet_id,
        outlet_desc::varchar(100) as outlet_desc,
        outlet_type1::varchar(100) as outlet_type1,
        outlet_type2::varchar(100) as outlet_type2,
        outlet_type3::varchar(100) as outlet_type3,
        outlet_type4::varchar(100) as outlet_type4,
        town::varchar(100) as town,
        cust_year::varchar(50) as cust_year,
        slsmn_cd::varchar(50) as slsmn_cd,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from  union_final
)

select * from final