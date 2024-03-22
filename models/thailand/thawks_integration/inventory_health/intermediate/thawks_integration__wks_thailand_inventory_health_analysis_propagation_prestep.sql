with wks_thailand_siso_propagate_final as
(
    select * from {{ref('thawks_integration__wks_thailand_siso_propagate_final')}}
),
edw_vw_th_material_dim as
(
    select * from {{ref('thaedw_integration__edw_vw_th_material_dim')}}
),
edw_material_dim as
(
    select * from {{ref('aspedw_integration__edw_material_dim')}}
),
edw_vw_th_customer_dim as
(
   select * from {{ref('thaedw_integration__edw_vw_th_customer_dim')}}
),
vw_edw_reg_exch_rate as
(
    select * from {{ref('aspedw_integration__vw_edw_reg_exch_rate')}}
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
onsesea as 
(
    SELECT 
        cal.year as year,
        cast(cal.qrtr_no as varchar) as year_quarter,
        cast(cal.mnth_id as varchar) as month_year,
        cal.mnth_no as month_number,
        cast('Thailand' as varchar) as country_name,
        t3.sap_prnt_cust_key as distributor_id,
        dstr_nm as distributor_id_name,
        trim(nvl(nullif(t4.gph_prod_frnchse, ''), 'NA')) as franchise,
        trim(nvl(nullif(t4.gph_prod_brnd, ''), 'NA')) as brand,
        trim(nvl(nullif(t4.gph_prod_sub_brnd, ''), 'NA')) as prod_sub_brand,
        trim(nvl(nullif(t4.gph_prod_vrnt, ''), 'NA')) as variant,
        trim(nvl(nullif(t4.gph_prod_sgmnt, ''), 'NA')) as segment,
        trim(nvl(nullif(t4.gph_prod_subsgmnt, ''), 'NA')) as prod_subsegment,
        trim(nvl(nullif(t4.gph_prod_ctgry, ''), 'NA')) as prod_category,
        trim(nvl(nullif(t4.gph_prod_subctgry, ''), 'NA')) as prod_subcategory,
        --t4.gph_prod_put_up_desc as put_up_description,
        trim(nvl(nullif(t4.pka_size_desc, ''), 'NA')) as pka_size_desc,
        ltrim(t4.sku_cd, 0) as sku_cd,
        t4.sap_mat_desc as sku_description,
        --trim(nvl(nullif(t4.greenlight_sku_flag,''),'NA')) as greenlight_sku_flag,
        trim(nvl(nullif(t4.pka_product_key, ''), 'NA')) as pka_product_key,
        trim(
            nvl(nullif(t4.pka_product_key_description, ''), 'NA')
        ) as pka_product_key_description,
        trim(nvl(nullif(t4.product_key, ''), 'NA')) as product_key,
        trim(nvl(nullif(t4.product_key_description, ''), 'NA')) as product_key_description,
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
            (sum(last_3months_so_value) * th_cur.exch_rate) as numeric(38, 5)
        ) as last_3months_so_val_usd,
        cast(
            (sum(last_6months_so_value) * th_cur.exch_rate) as numeric(38, 5)
        ) as last_6months_so_val_usd,
        cast(
            (sum(last_12months_so_value) * th_cur.exch_rate) as numeric(38, 5)
        ) as last_12months_so_val_usd,
        propagate_flag,
        propagate_from,
        case
            when propagate_flag = 'N' then 'Not propagate'
            else reason
        end as reason,
        replicated_flag,
        sum(sell_in_qty) as si_sls_qty,
        sum(sell_in_value) as si_gts_val,
        (sum(sell_in_value * th_cur.exch_rate)) as si_gts_val_usd,
        sum(inv_qty) as inventory_quantity,
        sum(inv_value) as inventory_val,
        sum(inv_value * th_cur.exch_rate) as inventory_val_usd,
        sum(so_qty) as so_sls_qty,
        sum(so_value) as so_grs_trd_sls,
        round(sum(so_value * th_cur.exch_rate)) as so_grs_trd_sls_usd
    FROM 
    (
        Select * from wks_thailand_siso_propagate_final
    ) siso,
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
                from  edw_vw_th_material_dim t4,edw_material_dim emd 
                where ltrim(t4.sap_matl_num, 0) = ltrim(emd.matl_num(+), 0)
            )
        )
        where rank = 1
    ) t4,
    (
        Select *
        from (
                Select distinct 
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
                    row_number() over (
                        partition by sap_prnt_cust_key --,sap_prnt_cust_desc,sap_go_to_mdl_key 
                        order by sap_cust_id --,sap_prnt_cust_desc,sap_go_to_mdl_key
                    ) as rank
                from 
                (
                    select * from edw_vw_th_customer_dim where sap_prnt_cust_key != ''
                ) AS T3
            )
        where rank = 1
    ) t3,
    (
        select distinct cal_year as year,
            cal_qrtr_no as qrtr_no,
            cal_mnth_id as mnth_id,
            cal_mnth_no as mnth_no
        from edw_vw_os_time_dim
    ) cal,
    (
        SELECT * FROM vw_edw_reg_exch_rate
        where cntry_key = 'TH'
            and to_ccy = 'USD'
            and jj_mnth_id =(
                select max(jj_mnth_id) from vw_edw_reg_exch_rate
            )
    ) th_cur
    where left(siso.month, 4) >=(date_part(year, current_date) -2)
        and siso.sap_parent_customer_key = t3.sap_prnt_cust_key(+)
        and ltrim(siso.matl_num, 0) = ltrim(t4.sku_cd(+), 0)
        and siso.month = cal.mnth_id
    group by 
        cal.year,
        cal.qrtr_no,
        cal.mnth_id,
        cal.mnth_no,
        dstr_nm,
        t3.sap_prnt_cust_key,
        t4.gph_prod_frnchse,
        t4.gph_prod_brnd,
        t4.gph_prod_sub_brnd,
        t4.gph_prod_vrnt,
        t4.gph_prod_sgmnt,
        t4.gph_prod_subsgmnt,
        t4.gph_prod_ctgry,
        t4.gph_prod_subctgry,
        t4.pka_size_desc,
        ltrim(t4.sku_cd, 0),
        t4.sap_mat_desc,
        t4.pka_product_key,
        t4.pka_product_key_description,
        t4.product_key,
        t4.product_key_description,
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
    where country_name || sap_prnt_cust_desc || month_year in (
            select country_name || sap_prnt_cust_desc || month_year as inclusion
            from (
                    select country_name,
                        sap_prnt_cust_desc,
                        month_year,
                        nvl(sum(inventory_val), 0) as inv_val,
                        nvl(sum(so_grs_trd_sls), 0) as sellout_val
                    from onsesea
                    where sap_prnt_cust_desc is not null
                    group by country_name,
                        sap_prnt_cust_desc,
                        month_year
                    having inv_val <> 0
                        and sellout_val <> 0
                )
        )
),
final as
(  
    Select 
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
        round(cast(si_sls_qty as numeric(38, 5)), 5) as si_sls_qty,
        round(cast(si_gts_val as numeric (38, 5)), 5) as si_gts_val,
        round(cast(si_gts_val_usd as numeric(38, 5)), 5) as si_gts_val_usd,
        round(cast (inventory_quantity as numeric(38, 5)), 5) as inventory_quantity,
        round(cast(inventory_val as numeric(38, 5)), 5) as inventory_val,
        round(cast (inventory_val_usd as numeric(38, 5)), 5) as inventory_val_usd,
        round(cast (so_sls_qty as numeric(38, 5)), 5) as so_sls_qty,
        round(cast (so_grs_trd_sls as numeric(38, 5)), 5) as so_grs_trd_sls,
        so_grs_trd_sls_usd::float as so_grs_trd_sls_usd,
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
    from regional
)
select * from final