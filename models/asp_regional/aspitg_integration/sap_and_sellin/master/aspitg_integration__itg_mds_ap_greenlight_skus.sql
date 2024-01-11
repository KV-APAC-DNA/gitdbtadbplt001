{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized= "table"
        )
}}

--Import CTE
with source as (
    select * from {{ ref('aspitg_integration__vw_stg_sdl_mds_ap_greenlight_skus') }}
),

--Logical CTE

--Final CTE
final as (
select
id::number(18,0) as id,
muid::varchar(50) as muid,
versionname::varchar(20) as versionname,
versionnumber::number(18,0) as versionnumber,
version_id::number(18,0) as version_id,
versionflag::varchar(5) as versionflag,
name::varchar(50) as name,
code::varchar(500) as code,
changetrackingmask::number(18,0) as changetrackingmask,
market::varchar(50) as market,
material_number::varchar(30) as material_number,
material_description::varchar(100) as material_description,
product_key_description::varchar(200) as product_key_description,
brand_name::varchar(50) as brand_name,
package::varchar(30) as package,
product_key::varchar(100) as product_key,
greenlight_sku_flag::varchar(5) as greenlight_sku_flag,
red_sku_flag::varchar(5) as red_sku_flag,
year::varchar(10) as year,
root_code::varchar(30) as root_code,
enterdatetime::timestamp_ntz(9) as enterdatetime,
enterusername::varchar(50) as enterusername,
enterversionnumber::number(18,0) as enterversionnumber,
lastchgdatetime::timestamp_ntz(9) as lastchgdatetime,
lastchgusername::varchar(50) as lastchgusername,
lastchgversionnumber::number(18,0) as lastchgversionnumber,
validationstatus::varchar(30) as validationstatus,
current_timestamp()::timestamp_ntz(9) as updt_dttm
from source
)

--Final select
select * from final 