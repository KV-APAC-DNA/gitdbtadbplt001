with itg_mds_ph_pos_pricelist as (
    select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_MDS_PH_POS_PRICELIST
),
itg_mds_ph_pos_product as (
    select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_MDS_PH_POS_PRODUCT
),
sdl_ph_pos_robinsons as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_robinsons') }}
),
sdl_ph_pos_mercury as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_mercury') }}
),
sdl_ph_pos_rustans as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_rustans') }}
),
sdl_ph_pos_south_star as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_south_star') }}
),
sdl_ph_pos_waltermart as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_waltermart') }}
),
sdl_ph_pos_watsons as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_watsons') }}
),
sdl_ph_pos_dyna as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_dyna') }}
),
sdl_ph_pos_711 as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_711') }}
),
sdl_ph_pos_puregold as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_puregold') }}
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

       (cast(ippd.qty as numeric(15,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(15,4))*ipp2.lst_price_unit)*float8(12.0 / 112.0) as jj_vat_amt,

       (cast(ippd.qty as numeric(15,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(15,4))*ipp2.lst_price_unit)*float8(100.0 / 112.0) as jj_nts,

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

       ((cast(ippd.qty as numeric(20,4))*cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)))*cast(ipp2.lst_price_unit as numeric(20,4)))*float8(12.0 / 112.0) as jj_vat_amt,

       ((cast(ippd.qty as numeric(20,4))*cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)))*cast(ipp2.lst_price_unit as numeric(20,4)))*float8(100.0 / 112.0) as jj_nts,

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

       cast(ippd.amt as numeric(15,4))*float8(12.0 / 112.0) as pos_tax,

       cast(ippd.amt as numeric(15,4))*float8(100.0 / 112.0) as pos_nts,

       cast(ippd.jnj_pc_per_cust_unit as numeric(15,4)) as conv_factor,

       cast(ippd.qty as numeric(15,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(15,4)) as jj_qty_pc,

       ipp2.lst_price_unit as jj_item_prc_per_pc,

       (cast(ippd.qty as numeric(15,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(15,4)))*ipp2.lst_price_unit as jj_gts,

       ((cast(ippd.qty as numeric(15,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(15,4)))*ipp2.lst_price_unit)*float8(12.0 / 112.0) as jj_vat_amt,

       ((cast(ippd.qty as numeric(15,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(15,4)))*ipp2.lst_price_unit)*float8(100.0 / 112.0) as jj_nts,

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

       cast(ippd.amt as numeric(15,4))*float8(12.0 / 112.0) as pos_tax,

       cast(ippd.amt as numeric(15,4))*float8(100.0 / 112.0) as pos_nts,

       cast(ippd.jnj_pc_per_cust_unit as numeric(15,4)) as conv_factor,

       cast(ippd.qty as numeric(15,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(15,4)) as jj_qty_pc,

       ipp2.lst_price_unit as jj_item_prc_per_pc,

       (cast(ippd.qty as numeric(15,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(15,4)))*ipp2.lst_price_unit as jj_gts,

       ((cast(ippd.qty as numeric(15,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(15,4)))*ipp2.lst_price_unit)*float8(12.0 / 112.0) as jj_vat_amt,

       (cast(ippd.qty as numeric(15,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(15,4))*ipp2.lst_price_unit)*float8(100.0 / 112.0) as jj_nts,

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

and   trim(ipp2.jj_mnth_id(+)) = trim(ippd.pl_jj_mnth_id);
),
waltermart as (
    select upper(substring(ippd.file_nm,1,2)) as cust_cd,

       ippd.jj_mnth_id as jj_mnth_id,

       ippd.pos_prod_cd as item_cd,

       ippd.store_cd as brnch_cd,

       cast(ippd.qty as numeric(20,4)) as pos_qty,

       cast(ippd.amt as numeric(20,4)) as pos_gts,

       cast(ippd.amt as numeric(20,4)) / nullif(cast(ippd.qty as numeric(20,4)),0) as pos_item_prc,

       cast(ippd.amt as numeric(20,4))*float8(12.0 / 112.0) as pos_tax,

       cast(ippd.amt as numeric(20,4))*float8(100.0 / 112.0) as pos_nts,

       cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)) as conv_factor,

       cast(ippd.qty as numeric(20,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)) as jj_qty_pc,

       cast(ipp2.lst_price_unit as numeric(20,4)) as jj_item_prc_per_pc,

       (cast(ippd.qty as numeric(20,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)))*cast(ipp2.lst_price_unit as numeric(20,4)) as jj_gts,

       ((cast(ippd.qty as numeric(20,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)))*cast(ipp2.lst_price_unit as numeric(20,4)))*float8(12.0 / 112.0) as jj_vat_amt,

       (cast(ippd.qty as numeric(20,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)))*cast(ipp2.lst_price_unit as numeric(20,4))*float8(100.0 / 112.0) as jj_nts,

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

       cast(ippd.amt as numeric(20,4))*float8(12.0 / 112.0) as pos_tax,

       cast(ippd.amt as numeric(20,4))*float8(100.0 / 112.0) as pos_nts,

       cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)) as conv_factor,

       cast(ippd.qty as numeric(20,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)) as jj_qty_pc,

       cast(ipp2.lst_price_unit as numeric(20,4)) as jj_item_prc_per_pc,

       (cast(ippd.qty as numeric(20,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)))*cast(ipp2.lst_price_unit as numeric(20,4)) as jj_gts,

       ((cast(ippd.qty as numeric(20,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)))*cast(ipp2.lst_price_unit as numeric(20,4)))*float8(12.0 / 112.0) as jj_vat_amt,

       ((cast(ippd.qty as numeric(20,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)))*cast(ipp2.lst_price_unit as numeric(20,4)))*float8(100.0 / 112.0) as jj_nts,

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
dyna as (
    select upper(substring(file_nm,1,4)) as cust_cd,

       ippd.jj_mnth_id as jj_mnth_id,

       ippd.pos_prod_cd as item_cd,

       ippd.store_cd as brnch_cd,

       cast(ippd.qty as numeric(15,4)) as pos_qty,

       cast(ippd.qty as numeric(15,4))*cast(ippd.cust_item_prc as numeric(15,4)) as pos_gts,

       cast(ippd.cust_item_prc as numeric(15,4)) as pos_item_prc,

       cast(ippd.qty as numeric(15,4))*cast(ippd.cust_item_prc as numeric(15,4))*float8(12.0 / 112.0) as pos_tax,

       cast(ippd.qty as numeric(15,4))*cast(ippd.cust_item_prc as numeric(15,4))*float8(100.0 / 112.0) as pos_nts,

       cast(ippd.cust_conv_factor as numeric(15,4)) as conv_factor,

       cast(ippd.qty as numeric(15,4)) / cast(ippd.cust_conv_factor as numeric(15,4)) as jj_qty_pc,

       ipp2.lst_price_unit as jj_item_prc_per_pc,

       cast(ippd.qty as numeric(15,4)) / cast(ippd.cust_conv_factor as numeric(15,4))*ipp2.lst_price_unit as jj_gts,

       (cast(ippd.qty as numeric(15,4)) / cast(ippd.cust_conv_factor as numeric(15,4))*ipp2.lst_price_unit)*float8(12.0 / 112.0) as jj_vat_amt,

       (cast(ippd.qty as numeric(15,4)) / cast(ippd.cust_conv_factor as numeric(15,4))*ipp2.lst_price_unit)*float8(100.0 / 112.0) as jj_nts,

       ippd.file_nm,

       ippd.cdl_dttm,

       current_timestamp() as crtd_dttm,

       null as updt_dttm

from itg_ph_pricelist ipp2,

     (select ipppd.item_cd,

             cust_cd,

             sap_item_cd,

             cust_conv_factor,

             jnj_pc_per_cust_unit,

             cust_item_prc,

             spm.jj_mnth_id,

             case

               when spm.jj_mnth_id < ipp.min_period and ipppd.early_bk_period is not null then ipppd.early_bk_period

               when spm.jj_mnth_id < ipp.min_period and ipppd.early_bk_period is null then ipp.min_period

               when spm.jj_mnth_id > ipp.max_period and ipppd.lst_period is not null then ipppd.lst_period

               when spm.jj_mnth_id > ipp.max_period and ipppd.lst_period is null then ipp.max_period

               else spm.jj_mnth_id

             end as pl_jj_mnth_id,

             spm.pos_prod_cd,

             spm.store_cd,

             spm.qty,

             spm.file_nm,

             spm.cdl_dttm

      from (select distinct item_cd,

                   cust_cd,

                   sap_item_cd,

                   cust_conv_factor,

                   jnj_pc_per_cust_unit,

                   cust_item_prc,

                   lst_period,

                   early_bk_period

            from itg_ph_pos_product_dim

            where cust_cd = 'DYNA') ipppd,

           (select max(jj_mnth_id) as max_period,

                   min(jj_mnth_id) as min_period,

                   item_cd

            from itg_ph_pricelist

            group by item_cd) ipp,

           sdl_ph_pos_dyna spm

      where upper(trim(ipppd.item_cd(+))) = upper(trim(spm.pos_prod_cd))

      and   upper(trim(ipp.item_cd(+))) = upper(ipppd.sap_item_cd)) ippd

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

       cast(ippd.tot_amt as numeric(15,4))*float8(12.0 / 112.0) as pos_tax,

       cast(ippd.tot_amt as numeric(15,4))*float8(100.0 / 112.0) as pos_nts,

       cast(ippd.cust_conv_factor as numeric(15,4)) as conv_factor,

       cast(ippd.tot_qty as numeric(15,4)) / cast(ippd.cust_conv_factor as numeric(15,4)) as jj_qty_pc,

       ipp2.lst_price_unit as jj_item_prc_per_pc,

       cast(ippd.tot_qty as numeric(15,4)) / cast(ippd.cust_conv_factor as numeric(15,4))*ipp2.lst_price_unit as jj_gts,

       (cast(ippd.tot_qty as numeric(15,4)) / cast(ippd.cust_conv_factor as numeric(15,4))*ipp2.lst_price_unit)*float8(12.0 / 112.0) as jj_vat_amt,

       (cast(ippd.tot_qty as numeric(15,4)) / cast(ippd.cust_conv_factor as numeric(15,4))*ipp2.lst_price_unit)*float8(100.0 / 112.0) as jj_nts,

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

       cast(ippd.qty as numeric(20,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)) as jj_qty_pc,

       cast(ipp2.lst_price_unit as numeric(20,4)) as jj_item_prc_per_pc,

       (cast(ippd.qty as numeric(20,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)))*cast(ipp2.lst_price_unit as numeric(20,4)) as jj_gts,

       ((cast(ippd.qty as numeric(20,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)))*cast(ipp2.lst_price_unit as numeric(20,4)))*float8(12.0 / 112.0) as jj_vat_amt,

       ((cast(ippd.qty as numeric(20,4)) / cast(ippd.jnj_pc_per_cust_unit as numeric(20,4)))*cast(ipp2.lst_price_unit as numeric(20,4)))*float8(100.0 / 112.0) as jj_nts,

       ippd.file_nm,

       ippd.cdl_dttm,

       current_timestamp() as crtd_dttm,

       null as updt_dttm

from itg_ph_pricelist ipp2,

     (select ipppd.item_cd,

             cust_cd,

             sap_item_cd,

             cust_conv_factor,

             case

               when (jnj_pc_per_cust_unit != 0 or jnj_pc_per_cust_unit != null) then jnj_pc_per_cust_unit

               else 1

             end as jnj_pc_per_cust_unit,

             spm.jj_mnth_id,

             case

               when spm.jj_mnth_id < ipp.min_period and ipppd.early_bk_period is not null then ipppd.early_bk_period

               when spm.jj_mnth_id < ipp.min_period and ipppd.early_bk_period is null then ipp.min_period

               when spm.jj_mnth_id > ipp.max_period and ipppd.lst_period is not null then ipppd.lst_period

               when spm.jj_mnth_id > ipp.max_period and ipppd.lst_period is null then ipp.max_period

               else spm.jj_mnth_id

             end as pl_jj_mnth_id,

             spm.pos_prod_cd,

             spm.store_cd,

             spm.qty,

             spm.file_nm,

             spm.cdl_dttm

      from (select distinct item_cd,

                   cust_cd,

                   sap_item_cd,

                   cust_conv_factor,

                   jnj_pc_per_cust_unit,

                   lst_period,

                   early_bk_period

            from itg_ph_pos_product_dim

            where cust_cd = 'PG') ipppd,

           (select max(jj_mnth_id) as max_period,

                   min(jj_mnth_id) as min_period,

                   item_cd

            from itg_ph_pricelist

            group by item_cd) ipp,

           (select * from sdl_ph_pos_puregold where qty <> 0) spm

      where upper(trim(ipppd.item_cd(+))) = upper(trim(spm.pos_prod_cd))

      and   upper(trim(ipp.item_cd(+))) = upper(ipppd.sap_item_cd)) ippd

where upper(trim(ipp2.item_cd(+))) = upper(ippd.sap_item_cd)

and   trim(ipp2.jj_mnth_id(+)) = trim(ippd.pl_jj_mnth_id)
)