with wks_thailand_siso_propagate_final as
(
    select * from thawks_integration.wks_thailand_siso_propagate_final
),
edw_vw_th_material_dim as
(
    select * from snaposeedw_integration.edw_vw_os_material_dim
    where cntry_key = 'TH'
),
edw_material_dim as
(
    select * from snapaspedw_integration.edw_material_dim
),
edw_vw_th_customer_dim as
(
    select * from snaposeedw_integration.edw_vw_os_customer_dim
    where sap_cntry_cd = 'TH'
),
wks_thailand_inventory_health_analysis_propagation_prestep as
(
    select * from thawks_integration.wks_thailand_inventory_health_analysis_propagation_prestep
),
vw_edw_reg_exch_rate as
(
    select * from snapaspedw_integration.vw_edw_reg_exch_rate
),
edw_copa_trans_fact as
(
    select * from snapaspedw_integration.edw_copa_trans_fact
),
edw_company_dim as
(
    select * from snapaspedw_integration.edw_company_dim
),
v_edw_customer_sales_dim as
(
    select * from snapaspedw_integration.v_edw_customer_sales_dim
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
onsesea as 
(
    select 
        cal.year as year,
        cast(cal.qrtr_no as varchar) as year_quarter,
        cast(cal.mnth_id as varchar) as month_year,
        cal.mnth_no as month_number,
        cast('Thailand' as varchar) as country_name,
        t3.sap_prnt_cust_key as distributor_id,
        dstr_nm as distributor_id_name,
        trim(coalesce(nullif(t4.gph_prod_frnchse, ''), 'NA')) as franchise,
        trim(coalesce(nullif(t4.gph_prod_brnd, ''), 'NA')) as brand,
        trim(coalesce(nullif(t4.gph_prod_sub_brnd, ''), 'NA')) as prod_sub_brand,
        trim(coalesce(nullif(t4.gph_prod_vrnt, ''), 'NA')) as variant,
        trim(coalesce(nullif(t4.gph_prod_sgmnt, ''), 'NA')) as segment,
        trim(coalesce(nullif(t4.gph_prod_subsgmnt, ''), 'NA')) as prod_subsegment,
        trim(coalesce(nullif(t4.gph_prod_ctgry, ''), 'NA')) as prod_category,
        trim(coalesce(nullif(t4.gph_prod_subctgry, ''), 'NA')) as prod_subcategory,
        trim(coalesce(nullif(t4.pka_size_desc, ''), 'NA')) as pka_size_desc,
        ltrim(t4.sku_cd, 0) as sku_cd,
        t4.sap_mat_desc as sku_description,
        trim(coalesce(nullif(t4.pka_product_key, ''), 'NA')) as pka_product_key,
        trim(
            coalesce(nullif(t4.pka_product_key_description, ''), 'NA')
        ) as pka_product_key_description,
        trim(coalesce(nullif(t4.product_key, ''), 'NA')) as product_key,
        trim(
            coalesce(nullif(t4.product_key_description, ''), 'NA')
        ) as product_key_description,
        th_cur.from_ccy,
        th_cur.to_ccy,
        th_cur.exch_rate,
        t3.sap_prnt_cust_key,
        t3.sap_prnt_cust_desc,
        t3.sap_cust_chnl_key,
        t3.sap_cust_chnl_desc,
        t3.sap_cust_sub_chnl_key,
        t3.sap_sub_chnl_desc,
        t3.sap_go_to_mdl_key,
        t3.sap_go_to_mdl_desc,
        t3.sap_bnr_key,
        t3.sap_bnr_desc,
        t3.sap_bnr_frmt_key,
        t3.sap_bnr_frmt_desc,
        t3.retail_env,
        t3.sap_region as region,
        t3.sap_region as zone_or_area,
        sum(last_3months_so) as last_3months_so_qty,
        sum(last_6months_so) as last_6months_so_qty,
        sum(last_12months_so) as last_12months_so_qty,
        sum(last_3months_so_value) as last_3months_so_val,
        sum(last_6months_so_value) as last_6months_so_val,
        sum(last_12months_so_value) as last_12months_so_val,
        sum(last_36months_so_value) as last_36months_so_val,
        cast(
            (
                sum(last_3months_so_value) * th_cur.exch_rate
            ) as decimal(38, 5)
        ) as last_3months_so_val_usd,
        cast(
            (
                sum(last_6months_so_value) * th_cur.exch_rate
            ) as decimal(38, 5)
        ) as last_6months_so_val_usd,
        cast(
            (
                sum(last_12months_so_value) * th_cur.exch_rate
            ) as decimal(38, 5)
        ) as last_12months_so_val_usd,
        propagate_flag,
        propagate_from,
        CASE
            WHEN propagate_flag = 'N' THEN 'Not propagate'
            else reason
        end as reason,
        replicated_flag,
        sum(sell_in_qty) as si_sls_qty,
        sum(sell_in_value) as si_gts_val,
        sum(sell_in_value * th_cur.exch_rate) as si_gts_val_usd,
        sum(inv_qty) as inventory_quantity,
        sum(inv_value) as inventory_val,
        sum(inv_value * th_cur.exch_rate) as inventory_val_usd,
        sum(so_qty) as so_sls_qty,
        sum(so_value) as so_grs_trd_sls,
        round(sum(so_value * th_cur.exch_rate)) as so_grs_trd_sls_usd
    FROM 
    wks_thailand_siso_propagate_final SISO,
    (
        select *
        from 
        (
            select *,
                row_number() over (partition by sku_cd order by sku_cd desc) as rank
            from 
            (
                select 
                    emd.pka_product_key as pka_product_key,
                    emd.pka_product_key_description as pka_product_key_description,
                    emd.pka_product_key as product_key,
                    emd.pka_product_key_description as product_key_description,
                    emd.pka_size_desc as pka_size_desc,
                    t4.gph_prod_frnchse,
                    t4.gph_prod_brnd,
                    t4.gph_prod_sub_brnd,
                    t4.gph_prod_vrnt,
                    t4.gph_prod_sgmnt,
                    t4.gph_prod_subsgmnt,
                    t4.gph_prod_ctgry,
                    t4.gph_prod_subctgry,
                    ltrim(t4.sap_matl_num) as sku_cd,
                    sap_mat_desc
                from edw_vw_th_material_dim t4, edw_material_dim emd
                where ltrim(t4.sap_matl_num, 0) = ltrim(emd.matl_num(+), 0)
            )
        )
        WHERE rank = 1
    ) AS t4,
    (
        SELECT *
        FROM 
        (
            SELECT DISTINCT T3.SAP_PRNT_CUST_KEY,
                T3.SAP_PRNT_CUST_DESC,
                T3.SAP_CUST_CHNL_KEY,
                T3.SAP_CUST_CHNL_DESC,
                T3.SAP_CUST_SUB_CHNL_KEY,
                T3.SAP_SUB_CHNL_DESC,
                T3.SAP_GO_TO_MDL_KEY,
                T3.SAP_GO_TO_MDL_DESC,
                T3.SAP_BNR_KEY,
                T3.SAP_BNR_DESC,
                T3.SAP_BNR_FRMT_KEY,
                T3.SAP_BNR_FRMT_DESC,
                T3.RETAIL_ENV,
                t3.sap_region,
                ROW_NUMBER() OVER ( PARTITION BY sap_prnt_cust_key ORDER BY sap_prnt_cust_key ) AS rank
            FROM 
                (
                    SELECT *
                    FROM EDW_VW_TH_CUSTOMER_DIM
                    WHERE sap_prnt_cust_key <> ''
                ) AS T3
        )
        WHERE rank = 1
        ) AS t3,
        (
            SELECT DISTINCT cal_year AS year,
                cal_qrtr_no AS qrtr_no,
                cal_MNTH_ID AS mnth_id,
                cal_MNTH_NO AS mnth_no
            FROM EDW_VW_OS_TIME_DIM
        ) AS cal,
        (
            SELECT *
            FROM vw_edw_reg_exch_rate
            WHERE cntry_key = 'TH'
                AND TO_CCY = 'USD'
                AND JJ_MNTH_ID = (
                    SELECT MAX(JJ_MNTH_ID)
                    FROM vw_edw_reg_exch_rate
                )
        ) AS TH_CUR
    WHERE LEFT(SISO.month, 4) >= (
            DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 2
        )
        AND SISO.sap_parent_customer_key = t3.SAP_PRNT_CUST_KEY(+)
        AND LTRIM(SISO.matl_num, 0) = LTRIM(t4.SKU_CD(+), 0)
        AND SISO.month = cal.mnth_id
    GROUP BY 
        CAL.YEAR,
        cal.QRTR_NO,
        cal.MNTH_ID,
        cal.MNTH_NO,
        dstr_nm,
        T3.SAP_PRNT_CUST_KEY,
        T4.gph_prod_frnchse,
        T4.GPH_PROD_BRND,
        T4.GPH_PROD_SUB_BRND,
        T4.GPH_PROD_VRNT,
        T4.GPH_PROD_SGMNT,
        T4.GPH_PROD_SUBSGMNT,
        T4.GPH_PROD_CTGRY,
        T4.GPH_PROD_SUBCTGRY,
        t4.pka_size_desc,
        LTRIM(T4.SKU_cd, 0),
        T4.SAP_MAT_DESC,
        t4.pka_product_key,
        t4.pka_product_key_description,
        t4.product_key,
        t4.product_key_description,
        TH_CUR.FROM_CCY,
        TH_CUR.TO_CCY,
        TH_CUR.EXCH_RATE,
        T3.SAP_PRNT_CUST_KEY,
        T3.SAP_PRNT_CUST_DESC,
        T3.SAP_CUST_CHNL_KEY,
        T3.SAP_CUST_CHNL_DESC,
        T3.SAP_CUST_SUB_CHNL_KEY,
        T3.SAP_SUB_CHNL_DESC,
        T3.SAP_GO_TO_MDL_KEY,
        T3.SAP_GO_TO_MDL_DESC,
        T3.SAP_BNR_KEY,
        T3.SAP_BNR_DESC,
        T3.SAP_BNR_FRMT_KEY,
        T3.SAP_BNR_FRMT_DESC,
        T3.RETAIL_ENV,
        t3.sap_region,
        t3.sap_region,
        propagate_flag,
        propagate_from,
        reason,
        replicated_flag
),
regional as 
(
    select *,
        sum(si_gts_val) over (partition by country_name, year, month_year) as si_inv_db_val,
        sum(si_gts_val_usd) over (partition by country_name, year, month_year) as si_inv_db_val_usd
    from onsesea
    where country_name || sap_prnt_cust_desc || month_year in 
    (
        select country_name || sap_prnt_cust_desc || month_year as inclusion
        from 
        (
            select country_name,
                sap_prnt_cust_desc,
                month_year,
                coalesce(sum(inventory_val), 0) as inv_val,
                coalesce(sum(so_grs_trd_sls), 0) as sellout_val
            from onsesea
            where not sap_prnt_cust_desc is null
            group by country_name,
                sap_prnt_cust_desc,
                month_year
            having inv_val <> 0
                and sellout_val <> 0
        )
    )
),
regionalcurrency as 
(
    select 
        cntry_key,
        cntry_nm,
        rate_type,
        from_ccy,
        to_ccy,
        valid_date,
        jj_year,
        jj_mnth_id as mnth_id,
        (
            cast(exch_rate as decimal(15, 5))
        ) as exch_rate
    from vw_edw_reg_exch_rate
    where cntry_key = 'TH'
        and jj_mnth_id >= (
            date_part(year, current_timestamp()) - 2
        )
        and to_ccy = 'USD'
),
sellin_all as 
(
    select ctry_key,
        obj_crncy_co_obj,
        prnt_cust_key,
        caln_yr_mo,
        fisc_yr,
        (
            cast(gts as decimal(38, 15))
        ) as gts
    from 
    (
        select copa.ctry_key as ctry_key,
            obj_crncy_co_obj,
            cus_sales_extn.prnt_cust_key,
            substring(fisc_yr_per, 1, 4) || substring(fisc_yr_per, 6, 2) as caln_yr_mo,
            fisc_yr,
            sum(amt_obj_crncy) as gts
        from edw_copa_trans_fact as copa
            left join edw_company_dim as cmp on copa.co_cd = cmp.co_cd
            left join v_edw_customer_sales_dim as cus_sales_extn on copa.sls_org = cus_sales_extn.sls_org
            and copa.dstr_chnl = cast(cus_sales_extn.dstr_chnl as text)
            and copa.div = cus_sales_extn.div
            and copa.cust_num = cus_sales_extn.cust_num
        where cmp.ctry_group = 'Thailand'
            and left(fisc_yr_per, 4) >= (
                date_part(year, current_timestamp()) - 2
            )
            and not copa.cust_num is null
            and copa.acct_hier_shrt_desc = 'GTS'
            and amt_obj_crncy > 0
        group by 1,
            2,
            3,
            4,
            5
    )
),
available_customers as 
(
    select 
        month_year,
        country_name,
        sap_prnt_cust_key,
        sap_prnt_cust_desc,
        sum(si_gts_val) as si_gts_val,
        sum(si_sls_qty) as si_sls_qty
    from wks_thailand_inventory_health_analysis_propagation_prestep as inv
    wherE country_name IN ('Thailand')
    group by 1,
        2,
        3,
        4
    having (
            sum(inventory_quantity) <> 0
            or sum(inventory_val) <> 0
        )
    order by 1 desc,
        2,
        3,
        4
),
gts as (
    SELECT ctry_key,
        obj_crncy_co_obj,
        caln_yr_mo,
        fisc_yr,
        SUM(SI_ALL_DB_VAL) AS gts_value,
        SUM(
            CASE
                WHEN avail_customer IS NULL THEN 0
                ELSE si_all_db_val
            END
        ) AS si_inv_db_val
    FROM 
    (       
        SELECT a.ctry_key,
            a.obj_crncy_co_obj,
            a.caln_yr_mo,
            a.fisc_yr,
            a.prnt_cust_key AS total_customer,
            b.sap_prnt_cust_key AS avail_customer,
            SUM(gts) AS SI_ALL_DB_VAL
        FROM sellin_all AS a
            LEFT JOIN available_customers AS b ON b.month_year = a.caln_yr_mo
            AND a.prnt_cust_key = b.sap_prnt_cust_key
        GROUP BY 1,
            2,
            3,
            4,
            5,
            6
        ORDER BY 1 DESC,
            2,
            3,
            4
    )
    GROUP BY 1,
        2,
        3,
        4
),
copa as (
    select ctry_key,
        obj_crncy_co_obj,
        caln_yr_mo,
        fisc_yr,
        (
            cast(gts_value as decimal(38, 5))
        ) as gts,
        si_inv_db_val,
        case
            when ctry_key = 'TH' then cast(
                (gts_value * exch_rate) / 1000 as decimal(38, 5)
            )
        end as gts_usd,
        case
            when ctry_key = 'TH' then cast(
                (si_inv_db_val * exch_rate) / 1000 as decimal(38, 5)
            )
        end as si_inv_db_val_usd
    from gts,
        regionalcurrency
    where gts.obj_crncy_co_obj = regionalcurrency.from_ccy
        and regionalcurrency.mnth_id = 
        (
            select max(mnth_id) from regionalcurrency
        )
),
final as
( 
    select 
        year::number(18,0) as year,
        year_quarter::varchar(11) as year_quarter,
        month_year::varchar(11) as month_year,
        month_number::number(18,0) as month_number,
        country_name::varchar(8) as country_name,
        distributor_id::varchar(12) as distributor_id,
        distributor_id_name::varchar(50) as distributor_id_name,
        franchise::varchar(30) as franchise,
        brand::varchar(30) as brand,
        prod_sub_brand::varchar(100) as prod_sub_brand,
        variant::varchar(100) as variant,
        segment::varchar(50) as segment,
        prod_subsegment::varchar(100) as prod_subsegment,
        prod_category::varchar(50) as prod_category,
        prod_subcategory::varchar(50) as prod_subcategory,
        pka_size_desc::varchar(30) as put_up_description,
        sku_cd::varchar(40) as sku_cd,
        sku_description::varchar(100) as sku_description,
        pka_product_key::varchar(68) as pka_product_key,
        pka_product_key_description::varchar(255) as pka_product_key_description,
        product_key::varchar(68) as product_key,
        product_key_description::varchar(255) as product_key_description,
        from_ccy::varchar(5) as from_ccy,
        to_ccy::varchar(5) as to_ccy,
        exch_rate::number(15,5) as exch_rate,
        sap_prnt_cust_key::varchar(12) as sap_prnt_cust_key,
        sap_prnt_cust_desc::varchar(50) as sap_prnt_cust_desc,
        sap_cust_chnl_key::varchar(12) as sap_cust_chnl_key,
        sap_cust_chnl_desc::varchar(50) as sap_cust_chnl_desc,
        sap_cust_sub_chnl_key::varchar(12) as sap_cust_sub_chnl_key,
        sap_sub_chnl_desc::varchar(50) as sap_sub_chnl_desc,
        sap_go_to_mdl_key::varchar(12) as sap_go_to_mdl_key,
        sap_go_to_mdl_desc::varchar(50) as sap_go_to_mdl_desc,
        sap_bnr_key::varchar(12) as sap_bnr_key,
        sap_bnr_desc::varchar(50) as sap_bnr_desc,
        sap_bnr_frmt_key::varchar(12) as sap_bnr_frmt_key,
        sap_bnr_frmt_desc::varchar(50) as sap_bnr_frmt_desc,
        retail_env::varchar(50) as retail_env,
        region::varchar(150) as region,
        zone_or_area::varchar(150) as zone_or_area,
        round(cast(si_sls_qty as decimal(38, 5)), 5) as si_sls_qty,
        round(cast(si_gts_val as decimal(38, 5)), 5) as si_gts_val,
        round(cast(si_gts_val_usd as decimal(38, 5)), 5) as si_gts_val_usd,
        round(cast(inventory_quantity as decimal(38, 5)), 5) as inventory_quantity,
        round(cast(inventory_val as decimal(38, 5)), 5) as inventory_val,
        round(cast(inventory_val_usd as decimal(38, 5)), 5) as inventory_val_usd,
        round(cast(so_sls_qty as decimal(38, 5)), 5) as so_sls_qty,
        round(cast(so_grs_trd_sls as decimal(38, 5)), 5) as so_grs_trd_sls,
        so_grs_trd_sls_usd::float as so_grs_trd_sls_usd,
        round(cast(copa.gts as decimal(38, 5)), 5) as si_all_db_val,
        round(cast(copa.gts_usd as decimal(38, 5)), 5) as si_all_db_val_usd,
        round(cast(copa.si_inv_db_val as decimal(38, 5)), 5) as si_inv_db_val,
        round(
            cast(copa.si_inv_db_val_usd as decimal(38, 5)),
            5
        ) as si_inv_db_val_usd,
        last_3months_so_qty::float as last_3months_so_qty,
        last_6months_so_qty::float as last_6months_so_qty,
        last_12months_so_qty::float as last_12months_so_qty,
        last_3months_so_val::float as last_3months_so_val,
        last_3months_so_val_usd::number(38,5) as last_3months_so_val_usd,
        last_6months_so_val::float as last_6months_so_val,
        last_6months_so_val_usd::number(38,5) as last_6months_so_val_usd,
        last_12months_so_val::float as last_12months_so_val,
        last_12months_so_val_usd::number(38,5) as last_12months_so_val_usd,
        propagate_flag::varchar(1) as propagate_flag,
        propagate_from::number(18,0) as propagate_from,
        reason::varchar(100) as reason,
        last_36months_so_val::float as last_36months_so_val
    from regional,
        copa
    where regional.year = copa.fisc_yr
        and regional.month_year = copa.caln_yr_mo
        and regional.from_ccy = copa.obj_crncy_co_obj
)
select * from final