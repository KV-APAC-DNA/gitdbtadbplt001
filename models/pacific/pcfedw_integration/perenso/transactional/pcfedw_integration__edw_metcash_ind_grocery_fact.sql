
with itg_metcash_ind_grocery as (
    select * from {{ ref('pcfitg_integration__itg_metcash_ind_grocery') }}
),
edw_time_dim as (
    select * from {{ source('pcfedw_integration', 'edw_time_dim') }}
),
edw_perenso_prod_dim as (
    select * from {{ ref('pcfedw_integration__edw_perenso_prod_dim') }}
),
itg_material_uom as (
    select * from {{ ref('aspitg_integration__itg_material_uom') }}
),    
transformed as (
select sls.week_end_dt as cal_date,

       etd.jj_wk,

       etd.week_num,

       cast(etd.jj_mnth_id as varchar) month_number,

       sls.supp_id,

       sls.supp_name,

       sls.state,

       sls.banner_id,

       sls.banner,

       sls.customer_id,

       sls.customer,

       ltrim(sls.product_id,0) as product_id,

       sls.product,

       sls.gross_sales,

       sls.gross_cases,

       case

         when muom.to_uom is null then sls.gross_cases

         else sls.gross_cases*muom.to_uom

       end as gross_units,

       muom.unit,

       muom.base_uom,

       muom.from_uom,

       muom.to_uom

from itg_metcash_ind_grocery sls,

     (select cal_date,

             time_id,

             jj_wk,

             jj_mnth_id,

             dense_rank() over (partition by jj_mnth_id order by jj_wk,jj_mnth_id) as week_num

      from edw_time_dim) etd,

     (select prod_key,

             prod_id,

             prod_metcash_code,

             unit,

             base_uom,

             uomn1d as from_uom,

             uomz1d as to_uom

      from edw_perenso_prod_dim eppd,

           itg_material_uom imu

      where eppd.prod_metcash_code != 'NOT ASSIGNED'

      and   imu.unit = 'PCK'

      and   lpad(eppd.prod_id,18,0) = imu.material (+)) muom

where sls.week_end_dt = etd.cal_date(+)::date

and   ltrim(sls.product_id,0) = ltrim(muom.prod_metcash_code (+),0)
),
final as (
select
cal_date::timestamp_ntz(9) as cal_date,
jj_wk::varchar(10) as jj_wk,
week_num::varchar(10) as week_num,
month_number::varchar(10) as month_number,
supp_id::varchar(50) as supp_id,
supp_name::varchar(256) as supp_name,
state::varchar(256) as state,
banner_id::varchar(50) as banner_id,
banner::varchar(256) as banner,
customer_id::varchar(50) as customer_id,
customer::varchar(256) as customer,
product_id::varchar(50) as product_id,
product::varchar(256) as product,
gross_sales::number(20,4) as gross_sales,
gross_cases::number(20,4) as gross_cases,
gross_units::number(20,4) as gross_units,
unit::varchar(50) as unit,
base_uom::varchar(50) as base_uom,
from_uom::number(20,4) as from_uom,
to_uom::number(20,4) as to_uom
from transformed
)
select * from final

