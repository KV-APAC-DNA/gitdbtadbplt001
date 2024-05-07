{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "delete from {{this}} where UPPER(CUST_CD) ||JJ_MNTH_ID||LTRIM(BRNCH_CD,'0') ||LTRIM(ITEM_CD,'0')  in (
          select distinct upper(substring(file_nm,1,3)) ||jj_mnth_id||ltrim(store_cd,'0') ||ltrim(pos_prod_cd,'0') from {{ source('phlsdl_raw', 'sdl_ph_pos_robinsons') }} 
          union 
          select distinct upper(substring(file_nm,1,3)) ||jj_mnth_id||ltrim(store_cd,'0') ||nvl(ltrim(pos_prod_cd,'0'),'') from {{ source('phlsdl_raw', 'sdl_ph_pos_mercury') }} 
          union 
          select distinct upper(substring(file_nm,1,2)) ||jj_mnth_id||ltrim(store_cd,'0') ||ltrim(pos_prod_cd,'0') from {{ source('phlsdl_raw', 'sdl_ph_pos_rustans') }} 
          union 
          select distinct upper(substring(file_nm,1,2)) ||jj_mnth_id||ltrim(store_cd,'0') ||ltrim(pos_prod_cd,'0') from {{ source('phlsdl_raw', 'sdl_ph_pos_south_star') }} 
          union 
          select distinct upper(substring(file_nm,1,3)) ||jj_mnth_id||ltrim(store_cd,'0') ||ltrim(pos_prod_cd,'0') from {{ source('phlsdl_raw', 'sdl_ph_pos_watsons') }} 
          union 
          select distinct 'DYNA'||mnth_id||customer_id||matl_num from {{ source('phlsdl_raw', 'sdl_ph_pos_dyna_sales') }} 
          union 
          select distinct  'PSC' ||MNTH_ID||STORE_CD||ITEM_CD  from {{ source('phlsdl_raw', 'sdl_ph_pos_711') }} 
          union 
          select distinct UPPER(SUBSTRING(FILE_NM,1,2)) ||JJ_MNTH_ID||LTRIM(STORE_CD,'0') ||LTRIM(POS_PROD_CD,'0') from {{ source('phlsdl_raw', 'sdl_ph_pos_waltermart') }} WHERE VENDOR_CD = '6539'
          
         );"
    )
}}



with itg_mds_ph_pos_pricelist as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_pos_pricelist') }}
),
itg_mds_ph_pos_product as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_pos_product') }}
),
sdl_ph_pos_robinsons as (
    select * from {{ ref('phlwks_integration__wks_ph_pos_robinsons') }}
),
sdl_ph_pos_mercury as (
    select * from {{ ref('phlwks_integration__wks_ph_pos_mercury') }}
),
sdl_ph_pos_rustans as (
    select * from {{ ref('phlwks_integration__wks_ph_pos_rustans') }}
),
sdl_ph_pos_south_star as (
    select * from {{ ref('phlwks_integration__wks_ph_pos_south_star') }}
),
sdl_ph_pos_waltermart as (
    select * from {{ ref('phlwks_integration__wks_ph_pos_waltermart') }}
),
sdl_ph_pos_watsons as (
    select * from {{ ref('phlwks_integration__wks_ph_pos_watsons') }}
),
sdl_ph_pos_dyna_sales as (
    select * from {{ ref('phlwks_integration__wks_ph_pos_dyna_sales') }}
),
sdl_ph_pos_711 as (
    select * from {{ ref('phlwks_integration__wks_ph_pos_711') }}
),
itg_mds_ph_pos_pricelist as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_pos_pricelist') }}
),
itg_ph_pricelist as (
select * from {{ ref('phlitg_integration__itg_ph_pricelist') }}
),
itg_ph_711_product_dim as (
select * from {{ref('phlitg_integration__itg_ph_711_product_dim')}}
),
itg_ph_dyna_product_dim as (
select * from {{ref('phlitg_integration__itg_ph_dyna_product_dim')}}
),
robinsons as (
select upper(substring(ippd.file_nm,1,3)) as cust_cd,

       ippd.jj_mnth_id as jj_mnth_id,

       ippd.pos_prod_cd as item_cd,

       ippd.store_cd as brnch_cd,

       cast(ippd.qty as numeric(15,4)) as pos_qty,

       cast(ippd.amt as numeric(15,4)) as pos_gts,

       cast(ippd.cust_price_pc as numeric(15,4)) as pos_item_prc,

       cast(ippd.tax_amt as numeric(15,4)) as pos_tax,

       cast(ippd.net_amt as numeric(15,4)) as pos_nts,

       cast(ippd.jnj_pc_per_cust_unit as numeric(15,4)) as conv_factor,

       cast(ippd.qty as numeric(15,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(15,4)) as jj_qty_pc,

       ipp2.lst_price_unit as jj_item_prc_per_pc,

       (cast(ippd.qty as numeric(15,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(15,4)))*ipp2.lst_price_unit as jj_gts,

       (cast(ippd.qty as numeric(15,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(15,4))*ipp2.lst_price_unit)*(12.0 / 112.0) as jj_vat_amt,

       (cast(ippd.qty as numeric(15,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(15,4))*ipp2.lst_price_unit)*(100.0 / 112.0) as jj_nts,

       ippd.file_nm,

       ippd.cdl_dttm,

       current_timestamp() as crtd_dttm,

       null as updt_dttm

from (select * from itg_mds_ph_pos_pricelist where active='Y') as  ipp2,

     (select sales.cust_item_cd,

             cust_cd,

             sap_item_cd,

             cust_conv_factor,

             jnj_pc_per_cust_unit,

             sales.jj_mnth_id as jj_mnth_id,

             case

               when sales.mnth_id = ipp.jj_mnth_id then ipp.jj_mnth_id

               when sales.mnth_id != nvl (ipp.jj_mnth_id,'0') and sales.early_bk_period!='' and upper(sales.early_bk_period)!='NULL' then sales.early_bk_period

               when sales.mnth_id != nvl (ipp.jj_mnth_id,'0') and (sales.early_bk_period is null or upper(sales.early_bk_period)='NULL') then sales.lst_period

             end as pl_jj_mnth_id,

             sales.pos_prod_cd,

             sales.store_cd,

             sales.qty,

             sales.amt,

             sales.cust_price_pc,

             sales.tax_amt,

             sales.net_amt,

             sales.file_nm,

             sales.cdl_dttm

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

                  and   upper(cust_cd) = 'ROB') ipppd,

                 sdl_ph_pos_robinsons spm

            where upper(ltrim(ipppd.cust_item_cd(+),'0')) = upper(ltrim(spm.pos_prod_cd,'0'))

            and   ipppd.mnth_id(+) = spm.jj_mnth_id) as sales,

           (select * from itg_mds_ph_pos_pricelist where active='Y') as  ipp

      where ipp.jj_mnth_id(+) = sales.jj_mnth_id

      and   ipp.item_cd(+) = sales.sap_item_cd) as ippd

where upper(trim(ipp2.item_cd(+))) = upper(ippd.sap_item_cd)

and   trim(ipp2.jj_mnth_id(+)) = trim(ippd.pl_jj_mnth_id)
),
mercury as (
    select upper(substring(ippd.file_nm,1,3)) as cust_cd,

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

       ippd.cdl_dttm,

       current_timestamp() as crtd_dttm,

       null as updt_dttm

from (select * from itg_mds_ph_pos_pricelist where active='Y') as  ipp2,

     (select sales.cust_item_cd,

             'MDC' as cust_cd,

             sap_item_cd,

             cust_conv_factor,

             jnj_pc_per_cust_unit,

             sales.jj_mnth_id as jj_mnth_id,

             case

               when sales.mnth_id = ipp.jj_mnth_id then ipp.jj_mnth_id

               when sales.mnth_id != nvl (ipp.jj_mnth_id,'0') and sales.early_bk_period!='' and upper(sales.early_bk_period)!='NULL' then sales.early_bk_period

               when sales.mnth_id != nvl (ipp.jj_mnth_id,'0') and (sales.early_bk_period is null or upper(sales.early_bk_period)='NULL') then sales.lst_period

             end as pl_jj_mnth_id,

             sales.pos_prod_cd,

             sales.store_cd,

             sales.qty,

             sales.file_nm,

             sales.cdl_dttm

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

                  and   upper(cust_cd) = 'MDC') ipppd,

                 sdl_ph_pos_mercury spm

            where upper(trim(ipppd.cust_item_cd(+))) = upper(trim(spm.pos_prod_cd))

            and   ipppd.mnth_id(+) = spm.jj_mnth_id) as sales,

           (select * from itg_mds_ph_pos_pricelist where active='Y') as  ipp

      where ipp.jj_mnth_id(+) = sales.jj_mnth_id

      and   ipp.item_cd(+) = sales.sap_item_cd) as ippd

where upper(trim(ipp2.item_cd(+))) = upper(ippd.sap_item_cd)

and   trim(ipp2.jj_mnth_id(+)) = trim(ippd.pl_jj_mnth_id)
),
rustans as (
    (select upper(substring(ippd.file_nm,1,2)) as cust_cd,

       ippd.jj_mnth_id as jj_mnth_id,

       ippd.pos_prod_cd as item_cd,

       ippd.store_cd as brnch_cd,

       cast(ippd.qty as numeric(15,4)) as pos_qty,

       cast(ippd.amt as numeric(15,4)) as pos_gts,

       case

         when cast(ippd.qty as numeric(15,4)) <> 0 then cast(ippd.amt as numeric(15,4)) / cast(ippd.qty as numeric(15,4))

         else 0

       end as pos_item_prc,

       cast(ippd.amt as numeric(15,4))*(12.0 / 112.0) as pos_tax,

       cast(ippd.amt as numeric(15,4))*(100.0 / 112.0) as pos_nts,

       cast(ippd.jnj_pc_per_cust_unit as numeric(15,4)) as conv_factor,

       cast(ippd.qty as numeric(15,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(15,4)) as jj_qty_pc,

       ipp2.lst_price_unit as jj_item_prc_per_pc,

       (cast(ippd.qty as numeric(15,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(15,4)))*ipp2.lst_price_unit as jj_gts,

       ((cast(ippd.qty as numeric(15,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(15,4)))*ipp2.lst_price_unit)*(12.0 / 112.0) as jj_vat_amt,

       ((cast(ippd.qty as numeric(15,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(15,4)))*ipp2.lst_price_unit)*(100.0 / 112.0) as jj_nts,

       ippd.file_nm,

       ippd.cdl_dttm,

       current_timestamp() as crtd_dttm,

       null as updt_dttm

from (select * from itg_mds_ph_pos_pricelist where active='Y') as ipp2,

     (select sales.cust_item_cd,

             cust_cd,

             sap_item_cd,

             cust_conv_factor,

             jnj_pc_per_cust_unit,

             cust_item_prc,

             sales.jj_mnth_id,

             case

               when sales.mnth_id = ipp.jj_mnth_id then ipp.jj_mnth_id

               when sales.mnth_id != nvl (ipp.jj_mnth_id,'0') and sales.early_bk_period!='' and upper(sales.early_bk_period)!='NULL' then sales.early_bk_period

               when sales.mnth_id != nvl (ipp.jj_mnth_id,'0') and (sales.early_bk_period is null or upper(sales.early_bk_period)='NULL') then sales.lst_period

             end as pl_jj_mnth_id,

             sales.pos_prod_cd,

             sales.store_cd,

             sales.qty,

             sales.amt,

             sales.file_nm,

             sales.cdl_dttm

      from (select *

            from (select distinct mnth_id,

                         item_cd as cust_item_cd,

                         cust_cd,

                         sap_item_cd,

                         cust_conv_factor,

                         cust_item_prc,

                         jnj_pc_per_cust_unit,

                         lst_period,

                         early_bk_period

                  from itg_mds_ph_pos_product

                  where active = 'Y'

                  and   upper(cust_cd) in ('RS','WC','SW')) ipppd,

                 sdl_ph_pos_rustans spm

            where upper(trim(ipppd.cust_item_cd(+))) = upper(trim(spm.pos_prod_cd))

            and   ipppd.mnth_id(+) = spm.jj_mnth_id

            and ipppd.cust_cd(+)=upper(substring(file_nm,1,2))) as sales,

           (select * from itg_mds_ph_pos_pricelist where active='Y') as  ipp

      where ipp.jj_mnth_id(+) = sales.jj_mnth_id

      and   ipp.item_cd(+) = sales.sap_item_cd) as ippd

where upper(trim(ipp2.item_cd(+))) = upper(ippd.sap_item_cd)

and   trim(ipp2.jj_mnth_id(+)) = trim(ippd.pl_jj_mnth_id))
),
southstar as (
    select upper(substring(ippd.file_nm,1,2)) as cust_cd,

       ippd.jj_mnth_id as jj_mnth_id,

       ippd.pos_prod_cd as item_cd,

       ippd.store_cd as brnch_cd,

       cast(ippd.qty as numeric(20,2)) as pos_qty,

       cast(ippd.amt as numeric(15,4)) as pos_gts,

       case

         when cast(ippd.qty as numeric(15,4)) <> 0 then cast(ippd.amt as numeric(15,4)) / cast(ippd.qty as numeric(15,4))

         else 0

       end as pos_item_prc,

       cast(ippd.amt as numeric(15,4))*(12.0 / 112.0) as pos_tax,

       cast(ippd.amt as numeric(15,4))*(100.0 / 112.0) as pos_nts,

       cast(ippd.jnj_pc_per_cust_unit as numeric(15,4)) as conv_factor,

       cast(ippd.qty as numeric(15,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(15,4)) as jj_qty_pc,

       ipp2.lst_price_unit as jj_item_prc_per_pc,

       (cast(ippd.qty as numeric(15,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(15,4)))*ipp2.lst_price_unit as jj_gts,

       ((cast(ippd.qty as numeric(15,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(15,4)))*ipp2.lst_price_unit)*(12.0 / 112.0) as jj_vat_amt,

       (cast(ippd.qty as numeric(15,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(15,4))*ipp2.lst_price_unit)*(100.0 / 112.0) as jj_nts,

       ippd.file_nm,

       ippd.cdl_dttm,

       current_timestamp() as crtd_dttm,

       null as updt_dttm

from (select * from itg_mds_ph_pos_pricelist where active='Y') as ipp2,

     (select sales.cust_item_cd,

             cust_cd,

             sap_item_cd,

             cust_conv_factor,

             jnj_pc_per_cust_unit,

             cust_item_prc,

             sales.jj_mnth_id,

             case

               when sales.mnth_id = ipp.jj_mnth_id then ipp.jj_mnth_id

               when sales.mnth_id != nvl (ipp.jj_mnth_id,'0') and sales.early_bk_period!='' and upper(sales.early_bk_period)!='NULL' then sales.early_bk_period

               when sales.mnth_id != nvl (ipp.jj_mnth_id,'0') and (sales.early_bk_period is null or upper(sales.early_bk_period)='NULL') then sales.lst_period

             end as pl_jj_mnth_id,

             sales.pos_prod_cd,

             sales.store_cd,

             sales.qty,

             sales.amt,

             sales.file_nm,

             sales.cdl_dttm

      from (select *

            from (select distinct mnth_id,

                         item_cd as cust_item_cd,

                         cust_cd,

                         sap_item_cd,

                         cust_conv_factor,

                         jnj_pc_per_cust_unit,

                         cust_item_prc,

                         lst_period,

                         early_bk_period

                  from itg_mds_ph_pos_product

                  where active = 'Y'

                  and   upper(cust_cd) = 'SS') ipppd,

                 sdl_ph_pos_south_star spm

            where upper(trim(ipppd.cust_item_cd(+))) = upper(trim(spm.pos_prod_cd))

            and   ipppd.mnth_id(+) = spm.jj_mnth_id) as sales,

         (select * from itg_mds_ph_pos_pricelist where active='Y') as ipp

      where ipp.jj_mnth_id(+) = sales.jj_mnth_id

      and   ipp.item_cd(+) = sales.sap_item_cd) as ippd

where upper(trim(ipp2.item_cd(+))) = upper(ippd.sap_item_cd)

and   trim(ipp2.jj_mnth_id(+)) = trim(ippd.pl_jj_mnth_id)
),
waltermart as (
    select upper(substring(ippd.file_nm,1,2)) as cust_cd,

       ippd.jj_mnth_id as jj_mnth_id,

       ippd.pos_prod_cd as item_cd,

       ippd.store_cd as brnch_cd,

       cast(ippd.qty as numeric(20,4)) as pos_qty,

       cast(ippd.amt as numeric(20,4)) as pos_gts,

       cast(ippd.amt as numeric(20,4)) / nullif(cast(ippd.qty as numeric(20,4)),0) as pos_item_prc,

       cast(ippd.amt as numeric(20,4))*(12.0 / 112.0) as pos_tax,

       cast(ippd.amt as numeric(20,4))*(100.0 / 112.0) as pos_nts,

       cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)) as conv_factor,

       cast(ippd.qty as numeric(20,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)) as jj_qty_pc,

       cast(ipp2.lst_price_unit as numeric(20,4)) as jj_item_prc_per_pc,

       (cast(ippd.qty as numeric(20,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)))*cast(ipp2.lst_price_unit as numeric(20,4)) as jj_gts,

       ((cast(ippd.qty as numeric(20,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)))*cast(ipp2.lst_price_unit as numeric(20,4)))*(12.0 / 112.0) as jj_vat_amt,

       (cast(ippd.qty as numeric(20,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)))*cast(ipp2.lst_price_unit as numeric(20,4))*(100.0 / 112.0) as jj_nts,

       ippd.file_nm,

       ippd.cdl_dttm,

       current_timestamp() as crtd_dttm,

       null as updt_dttm

from (select * from itg_mds_ph_pos_pricelist where active='Y') as ipp2,

     (select sales.cust_item_cd,

             cust_cd,

             sap_item_cd,

             cust_conv_factor,

             jnj_pc_per_cust_unit,

             sales.jj_mnth_id,

             case

               when sales.mnth_id = ipp.jj_mnth_id then ipp.jj_mnth_id

               when sales.mnth_id != nvl (ipp.jj_mnth_id,'0') and sales.early_bk_period!='' and upper(sales.early_bk_period)!='NULL' then sales.early_bk_period

               when sales.mnth_id != nvl (ipp.jj_mnth_id,'0') and (sales.early_bk_period is null or upper(sales.early_bk_period)='NULL') then sales.lst_period

             end as pl_jj_mnth_id,

             sales.pos_prod_cd,

             sales.store_cd,

             sales.qty,

             sales.amt,

             sales.file_nm,

             sales.cdl_dttm

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

                  and   upper(cust_cd) = 'WM') ipppd,

                 (select *

                  from sdl_ph_pos_waltermart

                  where vendor_cd = '6256'

                  and   qty not like '0.000%'

                  and   amt not like '0.000%') spm

            where upper(trim(ipppd.cust_item_cd(+))) = upper(trim(spm.pos_prod_cd))

            and   ipppd.mnth_id(+) = spm.jj_mnth_id) as sales,

           (select * from itg_mds_ph_pos_pricelist where active='Y') as ipp

      where ipp.jj_mnth_id(+) = sales.jj_mnth_id

      and   ipp.item_cd(+) = sales.sap_item_cd) as ippd

where upper(trim(ipp2.item_cd(+))) = upper(ippd.sap_item_cd)

and   trim(ipp2.jj_mnth_id(+)) = trim(ippd.pl_jj_mnth_id)
),
watsons as (
    select upper(substring(ippd.file_nm,1,3)) as cust_cd,

       ippd.jj_mnth_id as jj_mnth_id,

       ippd.pos_prod_cd as item_cd,

       ippd.store_cd as brnch_cd,

       cast(ippd.qty as numeric(20,4)) as pos_qty,

       cast(ippd.amt as numeric(20,4)) as pos_gts,

       cast(ippd.amt as numeric(20,4)) / nullif(cast(ippd.qty as numeric(20,4)),0) as pos_item_prc,

       cast(ippd.amt as numeric(20,4))*(12.0 / 112.0) as pos_tax,

       cast(ippd.amt as numeric(20,4))*(100.0 / 112.0) as pos_nts,

       cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)) as conv_factor,

       cast(ippd.qty as numeric(20,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)) as jj_qty_pc,

       cast(ipp2.lst_price_unit as numeric(20,4)) as jj_item_prc_per_pc,

       (cast(ippd.qty as numeric(20,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)))*cast(ipp2.lst_price_unit as numeric(20,4)) as jj_gts,

       ((cast(ippd.qty as numeric(20,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)))*cast(ipp2.lst_price_unit as numeric(20,4)))*(12.0 / 112.0) as jj_vat_amt,

       ((cast(ippd.qty as numeric(20,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)))*cast(ipp2.lst_price_unit as numeric(20,4)))*(100.0 / 112.0) as jj_nts,

       ippd.file_nm,

       ippd.cdl_dttm,

       current_timestamp() as crtd_dttm,

       null as updt_dttm

from (select * from itg_mds_ph_pos_pricelist where active='Y') as ipp2,

     (select sales.cust_item_cd,

             cust_cd,

             sap_item_cd,

             cust_conv_factor,

             jnj_pc_per_cust_unit,

             sales.jj_mnth_id,

             case

               when sales.mnth_id = ipp.jj_mnth_id then ipp.jj_mnth_id

               when sales.mnth_id != nvl (ipp.jj_mnth_id,'0') and sales.early_bk_period!='' and upper(sales.early_bk_period)!='NULL' then sales.early_bk_period

               when sales.mnth_id != nvl (ipp.jj_mnth_id,'0') and (sales.early_bk_period is null or upper(sales.early_bk_period)='NULL') then sales.lst_period

             end as pl_jj_mnth_id,

             sales.pos_prod_cd,

             sales.store_cd,

             sales.qty,

             sales.amt,

             sales.file_nm,

             sales.cdl_dttm

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

                  and   upper(cust_cd) = 'WAT') ipppd,

                 sdl_ph_pos_watsons spm

            where upper(trim(ipppd.cust_item_cd(+))) = upper(trim(spm.pos_prod_cd))

            and   ipppd.mnth_id(+) = spm.jj_mnth_id) as sales,

           (select * from itg_mds_ph_pos_pricelist where active='Y') as ipp

      where ipp.jj_mnth_id(+) = sales.jj_mnth_id

      and   ipp.item_cd(+) = sales.sap_item_cd) as ippd

where upper(trim(ipp2.item_cd(+))) = upper(ippd.sap_item_cd)

and   trim(ipp2.jj_mnth_id(+)) = trim(ippd.pl_jj_mnth_id)
),
seveneleven as (
    select cust_cd,

       ippd.jj_mnth_id as jj_mnth_id,

       ippd.pos_prod_cd as item_cd,

       ippd.store_cd as brnch_cd,

       cast(ippd.tot_qty as numeric(15,4)) as pos_qty,

       cast(ippd.tot_amt as numeric(15,4)) as pos_gts,

       null as pos_item_prc,

       cast(ippd.tot_amt as numeric(15,4))*(12.0 / 112.0) as pos_tax,

       cast(ippd.tot_amt as numeric(15,4))*(100.0 / 112.0) as pos_nts,

       cast(ippd.cust_conv_factor as numeric(15,4)) as conv_factor,

       cast(ippd.tot_qty as numeric(15,4)) / cast(ippd.cust_conv_factor as numeric(15,4)) as jj_qty_pc,

       ipp2.lst_price_unit as jj_item_prc_per_pc,

       cast(ippd.tot_qty as numeric(15,4)) / cast(ippd.cust_conv_factor as numeric(15,4))*ipp2.lst_price_unit as jj_gts,

       (cast(ippd.tot_qty as numeric(15,4)) / cast(ippd.cust_conv_factor as numeric(15,4))*ipp2.lst_price_unit)*(12.0 / 112.0) as jj_vat_amt,

       (cast(ippd.tot_qty as numeric(15,4)) / cast(ippd.cust_conv_factor as numeric(15,4))*ipp2.lst_price_unit)*(100.0 / 112.0) as jj_nts,

       null as file_nm,

       null as cdl_dttm,

       current_timestamp() as crtd_dttm,

       null as updt_dttm

from itg_ph_pricelist ipp2,

     (select sales.cust_item_cd,

             'PSC' as cust_cd,

             sap_item_cd,

             cust_conv_factor,

             sales.mnth_id as jj_mnth_id,

             case

               when sales.mnth_id = ipp.jj_mnth_id then ipp.jj_mnth_id

               when sales.mnth_id != nvl (ipp.jj_mnth_id,'0') and sales.early_bk_period is not null then sales.early_bk_period

               when sales.mnth_id != nvl (ipp.jj_mnth_id,'0') and sales.early_bk_period is null then sales.lst_period

             end as pl_jj_mnth_id,

             sales.item_cd as pos_prod_cd,

             sales.store_cd,

             sales.tot_qty,

             sales.tot_amt

      from (select *

            from (select distinct yearmonth,

                         item_cd as cust_item_cd,

                         cust_cd,

                         sap_item_cd,

                         cust_conv_factor,

                         lst_period,

                         early_bk_period

                  from itg_ph_711_product_dim) ipppd,

                 sdl_ph_pos_711 spm

            where upper(trim(ipppd.cust_item_cd(+))) = upper(trim(spm.item_cd))

            and   ipppd.yearmonth(+) = spm.mnth_id) as sales,

           itg_ph_pricelist ipp

      where ipp.jj_mnth_id(+) = sales.mnth_id

      and   ipp.item_cd(+) = sales.sap_item_cd) as ippd

where upper(trim(ipp2.item_cd(+))) = upper(ippd.sap_item_cd)

and   trim(ipp2.jj_mnth_id(+)) = trim(ippd.pl_jj_mnth_id)
),
dyna as (
SELECT CUST_CD,

       IPPD.JJ_MNTH_ID AS JJ_MNTH_ID,

       IPPD.POS_PROD_CD AS ITEM_CD,

       IPPD.CUSTOMER_ID AS BRNCH_CD,

       CAST(IPPD.QTY AS NUMERIC(15,4)) AS POS_QTY,

       CAST(IPPD.SLS_AMT AS NUMERIC(15,4)) AS POS_GTS,

       CAST(IPPD.CUST_ITEM_PRC AS NUMERIC(15,4)) AS POS_ITEM_PRC,

       CAST(IPPD.SLS_AMT AS NUMERIC(15,4))*(12.0 / 112.0) AS POS_TAX,

       CAST(IPPD.SLS_AMT AS NUMERIC(15,4))*(100.0 / 112.0) AS POS_NTS,

       CAST(IPPD.CUST_CONV_FACTOR AS NUMERIC(15,4)) AS CONV_FACTOR,

       CAST(IPPD.QTY AS NUMERIC(15,4)) / CAST(IPPD.CUST_CONV_FACTOR AS NUMERIC(15,4)) AS JJ_QTY_PC,

       IPP2.LST_PRICE_UNIT AS JJ_ITEM_PRC_PER_PC,

       CAST(IPPD.QTY AS NUMERIC(15,4)) / CAST(IPPD.CUST_CONV_FACTOR AS NUMERIC(15,4))*IPP2.LST_PRICE_UNIT AS JJ_GTS,

       (CAST(IPPD.QTY AS NUMERIC(15,4)) / CAST(IPPD.CUST_CONV_FACTOR AS NUMERIC(15,4))*IPP2.LST_PRICE_UNIT)*(12.0 / 112.0) AS JJ_VAT_AMT,

       (CAST(IPPD.QTY AS NUMERIC(15,4)) / CAST(IPPD.CUST_CONV_FACTOR AS NUMERIC(15,4))*IPP2.LST_PRICE_UNIT)*(100.0 / 112.0) AS JJ_NTS,

       NULL AS FILE_NM,

       NULL AS CDL_DTTM,

       current_timestamp() AS CRTD_DTTM,

       NULL AS UPDT_DTTM

FROM ITG_PH_PRICELIST IPP2,

     (SELECT SALES.ITEM_CD,

             'DYNA' as CUST_CD,

             SAP_ITEM_CD,

             CUST_CONV_FACTOR,

             CUST_ITEM_PRC,

             SALES.MNTH_ID AS JJ_MNTH_ID,

             CASE

               when sales.mnth_id = ipp.jj_mnth_id then ipp.jj_mnth_id

               when sales.mnth_id != nvl(ipp.jj_mnth_id,'0')  and sales.early_bk_period is not null then sales.early_bk_period

               when sales.mnth_id != nvl(ipp.jj_mnth_id,'0') and sales.early_bk_period is null then sales.lst_period

             END AS PL_JJ_MNTH_ID,

             SALES.MATL_NUM AS POS_PROD_CD,

             SALES.CUSTOMER_ID,

             SALES.QTY,

             SALES.SLS_AMT

      FROM (SELECT *

            FROM (SELECT DISTINCT YEARMONTH,

                         ITEM_CD,

                         CUST_CD,

                         SAP_ITEM_CD,

                         CUST_CONV_FACTOR,

                         CUST_ITEM_PRC,

                         LST_PERIOD,

                         EARLY_BK_PERIOD

                  FROM ITG_PH_DYNA_PRODUCT_DIM) IPPPD,

                 SDL_PH_POS_DYNA_SALES SPM

            WHERE UPPER(TRIM(IPPPD.ITEM_CD(+))) = UPPER(TRIM(SPM.MATL_NUM))

            AND   IPPPD.YEARMONTH(+) = SPM.MNTH_ID) AS SALES,

           ITG_PH_PRICELIST IPP

      WHERE IPP.JJ_MNTH_ID(+) = SALES.MNTH_ID

      AND   IPP.ITEM_CD(+) = SALES.SAP_ITEM_CD) AS IPPD

WHERE UPPER(TRIM(IPP2.ITEM_CD(+))) = UPPER(IPPD.SAP_ITEM_CD)

AND   TRIM(IPP2.JJ_MNTH_ID(+)) = TRIM(IPPD.PL_JJ_MNTH_ID)
),
transformed as (
select * from robinsons
union all
select * from mercury
union all
select * from seveneleven
union all
select * from watsons
union all
select * from waltermart
union all
select * from rustans
union all
select * from southstar
union all
select * from dyna
),
final as (
select 
cust_cd::varchar(30) as cust_cd,
jj_mnth_id::varchar(30) as jj_mnth_id,
item_cd::varchar(30) as item_cd,
brnch_cd::varchar(50) as brnch_cd,
pos_qty::number(20,4) as pos_qty,
pos_gts::number(20,4) as pos_gts,
pos_item_prc::number(20,4) as pos_item_prc,
pos_tax::number(20,4) as pos_tax,
pos_nts::number(20,4) as pos_nts,
conv_factor::number(20,4) as conv_factor,
jj_qty_pc::number(20,4) as jj_qty_pc,
jj_item_prc_per_pc::number(20,4) as jj_item_prc_per_pc,
jj_gts::number(20,4) as jj_gts,
jj_vat_amt::number(20,4) as jj_vat_amt,
jj_nts::number(20,4) as jj_nts,
file_nm::varchar(150) as file_nm,
cdl_dttm::varchar(50) as cdl_dttm,
crtd_dttm::timestamp_ntz(9) as crtd_dttm,
updt_dttm::timestamp_ntz(9) as updt_dttm
from transformed
)
select * from final
