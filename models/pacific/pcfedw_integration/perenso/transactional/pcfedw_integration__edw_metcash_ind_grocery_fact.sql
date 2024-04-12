
with itg_metcash_ind_grocery as (
    select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_METCASH_IND_GROCERY
),
edw_time_dim as (
    select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_TIME_DIM
),
edw_perenso_prod_dim as (
    select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PERENSO_PROD_DIM
),
itg_material_uom as (
    select * from DEV_DNA_CORE.SNAPASPITG_INTEGRATION.ITG_MATERIAL_UOM
),    
final as (
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

       ltrim(sls.product_id,0),

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
)
select * from final

