with WKS_PERENSO_PROD_INTERMIDEATE as(
    select * from DEV_DNA_CORE.SNAPPCFWKS_INTEGRATION.WKS_PERENSO_PROD_INTERMIDEATE
),
ITG_PERENSO_PROD_MAPPING as(
    select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_PROD_MAPPING
),
ITG_PERENSO_PRODUCT as(
    select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_PRODUCT
),
transformed as(
    select
        ipp.prod_key::number(10,0) as prod_key,
        ipp.prod_desc::varchar(100) as prod_desc,
        ipp.prod_id::varchar(50) as prod_id,
        ipp.prod_ean::varchar(50) as prod_ean,
        coalesce(grp.prod_jj_franchise, 'NOT ASSIGNED')::varchar(100) as prod_jj_franchise,
        coalesce(grp.prod_jj_category, 'NOT ASSIGNED')::varchar(100) as prod_jj_category,
        coalesce(grp.prod_jj_brand, 'NOT ASSIGNED')::varchar(100) as prod_jj_brand,
        coalesce(grp.prod_sap_franchise, 'NOT ASSIGNED')::varchar(100) as prod_sap_franchise,
        coalesce(grp.prod_sap_profit_centre, 'NOT ASSIGNED')::varchar(100) as prod_sap_profit_centre,
        coalesce(grp.prod_sap_product_major, 'NOT ASSIGNED')::varchar(100) as prod_sap_product_major,
        coalesce(grp.prod_grocery_franchise, 'NOT ASSIGNED')::varchar(100) as prod_grocery_franchise,
        coalesce(grp.prod_grocery_category, 'NOT ASSIGNED')::varchar(100) as prod_grocery_category,
        coalesce(grp.prod_grocery_brand, 'NOT ASSIGNED')::varchar(100) as prod_grocery_brand,
        coalesce(grp.prod_active_nz_pharma, 'NOT ASSIGNED')::varchar(100) as prod_active_nz_pharma,
        coalesce(grp.prod_active_au_grocery, 'NOT ASSIGNED')::varchar(100) as prod_active_au_grocery,
        coalesce(grp.prod_active_metcash, 'NOT ASSIGNED')::varchar(100) as prod_active_metcash,
        coalesce(grp.prod_active_nz_grocery, 'NOT ASSIGNED')::varchar(100) as prod_active_nz_grocery,
        coalesce(grp.prod_active_au_pharma, 'NOT ASSIGNED')::varchar(100) as prod_active_au_pharma,
        coalesce(grp.prod_pbs, 'NOT ASSIGNED')::varchar(100) as prod_pbs,
        coalesce(grp.prod_ims_brand, 'NOT ASSIGNED')::varchar(100) as prod_ims_brand,
        coalesce(grp.prod_nz_code, 'NOT ASSIGNED')::varchar(100) as prod_nz_code,
        coalesce(grp.prod_metcash_code, 'NOT ASSIGNED')::varchar(100) as prod_metcash_code,
        coalesce(grp.prod_old_id, 'NOT ASSIGNED')::varchar(50) as prod_old_id,
        coalesce(grp.prod_old_ean, 'NOT ASSIGNED')::varchar(50) as prod_old_ean,
        coalesce(grp.prod_tax, 'NOT ASSIGNED')::varchar(50) as prod_tax,
        coalesce(grp.prod_bwp_aud, 'NOT ASSIGNED')::varchar(50) as prod_bwp_aud,
        coalesce(grp.prod_bwp_nzd, 'NOT ASSIGNED')::varchar(50) as prod_bwp_nzd
        from (select wpai.prod_key,
                    max(case when upper(wppm.column_name) = 'PROD_JJ_FRANCHISE' then wpai.prod_grp_desc else null end) as prod_jj_franchise,
                    max(case when upper(wppm.column_name) = 'PROD_JJ_CATEGORY' then wpai.prod_grp_desc else null end) as prod_jj_category,
                    max(case when upper(wppm.column_name) = 'PROD_JJ_BRAND' then wpai.prod_grp_desc else null end) as prod_jj_brand,
                    max(case when upper(wppm.column_name) = 'PROD_SAP_FRANCHISE' then wpai.prod_grp_desc else null end) as prod_sap_franchise,
                    max(case when upper(wppm.column_name) = 'PROD_SAP_PROFIT_CENTRE' then wpai.prod_grp_desc else null end) as prod_sap_profit_centre,
                    max(case when upper(wppm.column_name) = 'PROD_SAP_PRODUCT_MAJOR' then wpai.prod_grp_desc else null end) as prod_sap_product_major,
                    max(case when upper(wppm.column_name) = 'PROD_GROCERY_FRANCHISE' then wpai.prod_grp_desc else null end) as prod_grocery_franchise,
                    max(case when upper(wppm.column_name) = 'PROD_GROCERY_CATEGORY' then wpai.prod_grp_desc else null end) as prod_grocery_category,
                    max(case when upper(wppm.column_name) = 'PROD_GROCERY_BRAND' then wpai.prod_grp_desc else null end) as prod_grocery_brand,
                    max(case when upper(wppm.column_name) = 'PROD_ACTIVE_NZ_PHARMA' then wpai.prod_grp_desc else null end) as prod_active_nz_pharma,
                    max(case when upper(wppm.column_name) = 'PROD_ACTIVE_AU_GROCERY' then wpai.prod_grp_desc else null end) as prod_active_au_grocery,
                    max(case when upper(wppm.column_name) = 'PROD_ACTIVE_METCASH' then wpai.prod_grp_desc else null end) as prod_active_metcash,
                    max(case when upper(wppm.column_name) = 'PROD_ACTIVE_NZ_GROCERY' then wpai.prod_grp_desc else null end) as prod_active_nz_grocery,
                    max(case when upper(wppm.column_name) = 'PROD_ACTIVE_AU_PHARMA' then wpai.prod_grp_desc else null end) as prod_active_au_pharma,
                    max(case when upper(wppm.column_name) = 'PROD_PBS' then wpai.prod_grp_desc else null end) as prod_pbs,
                    max(case when upper(wppm.column_name) = 'PROD_IMS_BRAND' then wpai.prod_grp_desc else null end) as prod_ims_brand,
                    max(case when upper(wppm.column_name) = 'PROD_NZ_CODE' then wpai.prod_grp_desc else null end) as prod_nz_code,
                    max(case when upper(wppm.column_name) = 'PROD_METCASH_CODE' then wpai.prod_grp_desc else null end) as prod_metcash_code,
                    max(case when upper(wppm.column_name) = 'PROD_OLD_ID' then wpai.prod_grp_desc else null end) as prod_old_id,
                    max(case when upper(wppm.column_name) = 'PROD_OLD_EAN' then wpai.prod_grp_desc else null end) as prod_old_ean,
                    max(case when upper(wppm.column_name) = 'PROD_TAX' then wpai.prod_grp_desc else null end) as prod_tax,
                    max(case when upper(wppm.column_name) = 'PROD_BWP_AUD' then wpai.prod_grp_desc else null end) as prod_bwp_aud,
                    max(case when upper(wppm.column_name) = 'PROD_BWP_NZD' then wpai.prod_grp_desc else null end) as prod_bwp_nzd
            from itg_perenso_prod_mapping wppm,
                wks_perenso_prod_intermideate wpai
            where wppm.field_key = wpai.field_key(+)
            and   wppm.prod_grp_lev_key = wpai.prod_grp_lev_key(+)
            and   wppm.prod_grp_key = wpai.prod_grp_key(+)
            group by wpai.prod_key) grp,
            itg_perenso_product ipp
        where ipp.prod_key = grp.prod_key
)
select * from transformed