{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        pre_hook = "delete from {{this}} where file_nm in (select distinct file_name from (select distinct file_name from {{ source('phlsdl_raw', 'sdl_ph_pos_sm_goods') }}
        union all 
        select distinct file_name from {{ source('phlsdl_raw', 'sdl_ph_pos_puregold') }}) );"
    )
}}
with itg_mds_ph_pos_pricelist as (
        select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_MDS_PH_POS_PRICELIST
),
itg_mds_ph_pos_product as (
    select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_MDS_PH_POS_PRODUCT
),
sdl_ph_pos_puregold as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_puregold') }}
),
sdl_ph_pos_sm_goods as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_sm_goods') }}
),
puregold as (
select upper(substring(ippd.file_nm,1,2)) as cust_cd,

       ippd.jj_mnth_id as jj_mnth_id,

       ippd.pos_prod_cd as item_cd,

       ippd.store_cd as brnch_cd,

       cast(ippd.qty as numeric(20,4)) as pos_qty,

       null as pos_gts,

       null as pos_item_prc,

       null as pos_tax,

       null as pos_nts,

       cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)) as conv_factor,

       cast(ippd.qty as numeric(20,4))*cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)) as jj_qty_pc,

       cast(ipp2.lst_price_unit as numeric(20,4)) as jj_item_prc_per_pc,

       (cast(ippd.qty as numeric(20,4))*cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)))*cast(ipp2.lst_price_unit as numeric(20,4)) as jj_gts,

       ((cast(ippd.qty as numeric(20,4))*cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)))*cast(ipp2.lst_price_unit as numeric(20,4)))*(12.0 / 112.0) as jj_vat_amt,

       ((cast(ippd.qty as numeric(20,4))*cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)))*cast(ipp2.lst_price_unit as numeric(20,4)))*(100.0 / 112.0) as jj_nts,

       ippd.file_nm,

       current_timestamp() as crtd_dttm,

       current_timestamp() as updt_dttm

from (select *

      from itg_mds_ph_pos_pricelist

      where active = 'Y') as ipp2,

     (select sales.cust_item_cd,

             upper(substring(sales.file_nm,1,2)) as cust_cd,

             sap_item_cd,

             cust_conv_factor,

             jnj_pc_per_cust_unit,

             sales.jj_mnth_id as jj_mnth_id,

             case

               when sales.mnth_id = ipp.jj_mnth_id then ipp.jj_mnth_id

               when sales.mnth_id != nvl (ipp.jj_mnth_id,'0') and sales.early_bk_period != '' and upper(sales.early_bk_period) != 'NULL' then sales.early_bk_period

               when sales.mnth_id != nvl (ipp.jj_mnth_id,'0') and (sales.early_bk_period is null or upper(sales.early_bk_period) = 'NULL') then sales.lst_period

             end as pl_jj_mnth_id,

             sales.pos_prod_cd,

             sales.store_cd,

             sales.qty,

             sales.file_nm

      from (select *

            from (select distinct mnth_id,

                         item_cd as cust_item_cd,

                         cust_cd,

                         sap_item_cd,

                         cust_conv_factor,

                         jnj_pc_per_cust_unit,

                         lst_period,

                         early_bk_period

                  from itg_mds_ph_pos_product

                  where active = 'Y'

                  and   upper(cust_cd) = 'PG') ipppd,

                 (select split_part(split_part (file_name,'.',1),'_',2) as jj_mnth_id,

                         sku as pos_prod_cd,

                         to_store as store_cd,

                         qty,

                         file_name as file_nm

                  from sdl_ph_pos_puregold

				  ) spg

            where upper(trim(ipppd.cust_item_cd (+))) = upper(trim(spg.pos_prod_cd))

            and   ipppd.mnth_id (+) = spg.jj_mnth_id) as sales,

           (select *

            from itg_mds_ph_pos_pricelist

            where active = 'Y') as ipp

      where ipp.jj_mnth_id (+) = sales.jj_mnth_id

      and   ipp.item_cd (+) = sales.sap_item_cd) as ippd

where upper(trim(ipp2.item_cd (+))) = upper(ippd.sap_item_cd)

and   trim(ipp2.jj_mnth_id (+)) = trim(ippd.pl_jj_mnth_id)
),
goods as (
    select upper(substring(ippd.file_nm,1,2)) as cust_cd,

       ippd.jj_mnth_id as jj_mnth_id,

       ippd.pos_prod_cd as item_cd,

       ippd.store_cd as brnch_cd,

       cast(ippd.qty as numeric(20,4)) as pos_qty,

       null as pos_gts,

       null as pos_item_prc,

       null as pos_tax,

       null as pos_nts,

       cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)) as conv_factor,

       cast(ippd.qty as numeric(20,4))*cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)) as jj_qty_pc,

       cast(ipp2.lst_price_unit as numeric(20,4)) as jj_item_prc_per_pc,

       (cast(ippd.qty as numeric(20,4))*cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)))*cast(ipp2.lst_price_unit as numeric(20,4)) as jj_gts,

       ((cast(ippd.qty as numeric(20,4))*cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)))*cast(ipp2.lst_price_unit as numeric(20,4)))*(12.0 / 112.0) as jj_vat_amt,

       ((cast(ippd.qty as numeric(20,4))*cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)))*cast(ipp2.lst_price_unit as numeric(20,4)))*(100.0 / 112.0) as jj_nts,

       ippd.file_nm,

       current_timestamp() as crtd_dttm,

       current_timestamp() as updt_dttm

from (select *

      from itg_mds_ph_pos_pricelist

      where active = 'Y') as ipp2,

     (select sales.cust_item_cd,

             upper(substring(sales.file_nm,1,2)) as cust_cd,

             sap_item_cd,

             cust_conv_factor,

             jnj_pc_per_cust_unit,

             sales.jj_mnth_id as jj_mnth_id,

             case

               when sales.mnth_id = ipp.jj_mnth_id then ipp.jj_mnth_id

               when sales.mnth_id != nvl (ipp.jj_mnth_id,'0') and sales.early_bk_period != '' and upper(sales.early_bk_period) != 'NULL' then sales.early_bk_period

               when sales.mnth_id != nvl (ipp.jj_mnth_id,'0') and (sales.early_bk_period is null or upper(sales.early_bk_period) = 'NULL') then sales.lst_period

             end as pl_jj_mnth_id,

             sales.pos_prod_cd,

             sales.store_cd,

             sales.qty,

             sales.file_nm

      from (select *

            from (select distinct mnth_id,

                         item_cd as cust_item_cd,

                         cust_cd,

                         sap_item_cd,

                         cust_conv_factor,

                         jnj_pc_per_cust_unit,

                         lst_period,

                         early_bk_period

                  from itg_mds_ph_pos_product

                  where active = 'Y'

                  and   upper(cust_cd) = 'SM') ipppd,

                 (select split_part(split_part (file_name,'.',1),'_',3) as jj_mnth_id,

                         article_number as pos_prod_cd,

                         site_code as store_cd,

                         received_qty as qty,

                         file_name as file_nm

                  from sdl_ph_pos_sm_goods

				 ) ssm

            where upper(trim(ipppd.cust_item_cd (+))) = upper(trim(ssm.pos_prod_cd))

            and   ipppd.mnth_id (+) = ssm.jj_mnth_id) as sales,

           (select *

            from itg_mds_ph_pos_pricelist

            where active = 'Y') as ipp

      where ipp.jj_mnth_id (+) = sales.jj_mnth_id

      and   ipp.item_cd (+) = sales.sap_item_cd) as ippd

where upper(trim(ipp2.item_cd (+))) = upper(ippd.sap_item_cd)

and   trim(ipp2.jj_mnth_id (+)) = trim(ippd.pl_jj_mnth_id)
),
final as (
    select * from puregold
    union ALL
    select * from goods
)
select * from final