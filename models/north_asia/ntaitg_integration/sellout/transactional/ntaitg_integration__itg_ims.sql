{{
    config(
        materialized="incremental",
        incremental_strategy='append',
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where (ims_txn_dt,dstr_cd) in (select distinct transaction_date,distributor_code from {{ ref('ntaitg_integration__sdl_tw_ims_dstr_std_sel_out') }}) and ctry_cd = 'TW';
        delete from {{this}} as itg_ims using {{ ref('ntawks_integration__wks_itg_ims_sls') }} as wks_itg_ims_sls where itg_ims.ims_txn_dt = wks_itg_ims_sls.ims_txn_dt and itg_ims.cust_cd = wks_itg_ims_sls.cust_cd and itg_ims.prod_cd = wks_itg_ims_sls.prod_cd and itg_ims.doc_type = wks_itg_ims_sls.doc_type and wks_itg_ims_sls.chng_flg = 'U' and itg_ims.dstr_cd = '110256';
        delete from {{this}} as itg_ims using {{ ref('ntawks_integration__wks_itg_ims_sls') }} as wks_itg_ims_sls where itg_ims.ims_txn_dt = wks_itg_ims_sls.ims_txn_dt and itg_ims.cust_cd = wks_itg_ims_sls.cust_cd and itg_ims.prod_cd = wks_itg_ims_sls.prod_cd and itg_ims.doc_type = wks_itg_ims_sls.doc_type and wks_itg_ims_sls.chng_flg = 'U' and itg_ims.dstr_cd = '100681';
        {% endif %}"
    )
}}
with sdl_tw_ims_dstr_std_sel_out as (
    select * from {{ ref('ntaitg_integration__sdl_tw_ims_dstr_std_sel_out') }}
),
itg_ims_dstr_cust_attr as (
    select * from {{ ref('ntaitg_integration__itg_ims_dstr_cust_attr') }}
),
tw_ims_distributor_ingestion_metadata as (
    select * from {{ source('ntawks_integration', 'tw_ims_distributor_ingestion_metadata') }}
),
wks_itg_ims_sls as (
    select * from {{ ref('ntawks_integration__wks_itg_ims_sls') }}
),
tw_transformed as 
(    
    SELECT 
        transaction_date AS ims_txn_dt,
        sell_out.distributor_code AS dstr_cd,
        COALESCE(meta.dstr_nm, '#') AS dstr_nm,
        COALESCE(distributors_customer_code, '#') AS cust_cd,
        COALESCE(cust.dstr_cust_nm, '#') AS cust_nm,
        COALESCE(distributors_product_code, '#') AS prod_cd,
        distributors_product_name AS prod_nm,
        CAST(report_period_start_date AS DATE) AS rpt_per_strt_dt,
        CAST(report_period_end_date AS DATE) AS rpt_per_end_dt,
        COALESCE(ean, '#') AS ean_num,
        uom AS uom,
        unit_price AS unit_prc,
        ----(net_trade_sales + sls_amt) AS sls_amt,
        sales_amount AS sls_amt,
        ----(sales_order_quantity + sls_qty) AS sls_qty,
        ----sales_qty AS sls_qty,----changes done to handle decimal values in sales quantity field
        CASE
            WHEN charindex(sales_qty, '.') = 0 THEN sales_qty
            WHEN charindex(sales_qty, '.') > 0 THEN CAST(
                SUBSTRING(sales_qty, 0, charindex(sales_qty, '.')) AS INT
            )
        END AS sls_qty,
        ----CAST(return_qty AS INT) AS rtrn_qty,----changes done to handle decimal values in return quantity field
        CASE
            WHEN charindex(return_qty, '.') = 0 THEN return_qty
            WHEN charindex(return_qty, '.') > 0 THEN CAST(
                SUBSTRING(return_qty, 0, charindex(return_qty, '.')) AS INT
            )
        END AS rtrn_qty,
        return_amount AS rtrn_amt,
        NULL AS ship_cust_nm,
        NULL AS cust_cls_grp,
        NULL AS cust_sub_cls,
        NULL AS prod_spec,
        NULL AS itm_agn_nm,
        NULL AS ordr_co,
        NULL AS rtrn_rsn,
        NULL AS sls_ofc_cd,
        NULL AS sls_grp_cd,
        NULL AS sls_ofc_nm,
        NULL AS sls_grp_nm,
        NULL AS acc_type,
        NULL AS co_cd,
        sales_rep_code AS sls_rep_cd,
        sales_rep_name AS sls_rep_nm,
        CAST(NULL AS DATE) AS doc_dt,
        '#' AS doc_type,
        NULL AS doc_num,
        NULL AS invc_num,
        NULL AS remark_desc,
        CAST(NULL AS INT) AS gift_qty,
        NULL AS sls_bal_amt,
        NULL AS sku_per_box,
        'TW' AS ctry_cd,
        'TWD' AS crncy_cd,
        current_timestamp::timestamp_ntz(9) AS crt_dttm,
        current_timestamp::timestamp_ntz(9) AS updt_dttm
    FROM sdl_tw_ims_dstr_std_sel_out sell_out
    LEFT OUTER JOIN 
    (
        SELECT * FROM itg_ims_dstr_cust_attr
        WHERE ctry_cd = 'TW'
    ) cust 
    ON sell_out.distributor_code = cust.dstr_cd
    AND sell_out.distributors_customer_code = cust.dstr_cust_cd
    LEFT OUTER JOIN 
    (
        SELECT DISTINCT ctry_cd,
            distributor_code,
            dstr_nm
        FROM tw_ims_distributor_ingestion_metadata
        WHERE subject_area = 'Sell-Out'
            AND ctry_cd = 'TW'
    ) meta ON sell_out.distributor_code = meta.distributor_code
),
hk_transformed as (
    SELECT ims_txn_dt,
        dstr_cd,
        dstr_nm,
        cust_cd,
        cust_nm,
        prod_cd,
        prod_nm,
        rpt_per_strt_dt,
        rpt_per_end_dt,
        ean_num,
        uom,
        unit_prc,
        sls_amt,
        sls_qty,
        rtrn_qty,
        rtrn_amt,
        ship_cust_nm,
        cust_cls_grp,
        cust_sub_cls,
        prod_spec,
        itm_agn_nm,
        ordr_co,
        rtrn_rsn,
        sls_ofc_cd,
        sls_grp_cd,
        sls_ofc_nm,
        sls_grp_nm,
        acc_type,
        co_cd,
        sls_rep_cd,
        sls_rep_nm,
        doc_dt,
        doc_type,
        doc_num,
        invc_num,
        remark_desc,
        gift_qty,
        sls_bfr_tax_amt,
        sku_per_box,
        ctry_cd,
        crncy_cd,
        case when chng_flg = 'I' THEN convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) else tgt_crt_dttm end as crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm
    FROM wks_itg_ims_sls
),
transformed as (
    select * from tw_transformed
    union all
    select * from hk_transformed
),
final as 
(
    select 
        ims_txn_dt::date as ims_txn_dt,
        dstr_cd::varchar(10) as dstr_cd,
        dstr_nm::varchar(100) as dstr_nm,
        cust_cd::varchar(50) as cust_cd,
        cust_nm::varchar(100) as cust_nm,
        prod_cd::varchar(20) as prod_cd,
        prod_nm::varchar(100) as prod_nm,
        rpt_per_strt_dt::date as rpt_per_strt_dt,
        rpt_per_end_dt::date as rpt_per_end_dt,
        ean_num::varchar(20) as ean_num,
        uom::varchar(10) as uom,
        unit_prc::number(21,5) as unit_prc,
        sls_amt::number(21,5) as sls_amt,
        sls_qty::number(18,0) as sls_qty,
        rtrn_qty::number(18,0) as rtrn_qty,
        rtrn_amt::number(21,5) as rtrn_amt,
        ship_cust_nm::varchar(100) as ship_cust_nm,
        cust_cls_grp::varchar(20) as cust_cls_grp,
        cust_sub_cls::varchar(20) as cust_sub_cls,
        prod_spec::varchar(50) as prod_spec,
        itm_agn_nm::varchar(100) as itm_agn_nm,
        ordr_co::varchar(20) as ordr_co,
        rtrn_rsn::varchar(100) as rtrn_rsn,
        sls_ofc_cd::varchar(10) as sls_ofc_cd,
        sls_grp_cd::varchar(10) as sls_grp_cd,
        sls_ofc_nm::varchar(20) as sls_ofc_nm,
        sls_grp_nm::varchar(20) as sls_grp_nm,
        acc_type::varchar(10) as acc_type,
        co_cd::varchar(20) as co_cd,
        sls_rep_cd::varchar(20) as sls_rep_cd,
        sls_rep_nm::varchar(50) as sls_rep_nm,
        doc_dt::date as doc_dt,
        doc_type::varchar(10) as doc_type,
        doc_num::varchar(20) as doc_num,
        invc_num::varchar(15) as invc_num,
        remark_desc::varchar(100) as remark_desc,
        gift_qty::number(18,0) as gift_qty,
        sls_bal_amt::number(21,5) as sls_bfr_tax_amt,
        sku_per_box::number(21,5) as sku_per_box,
        ctry_cd::varchar(2) as ctry_cd,
        crncy_cd::varchar(3) as crncy_cd,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from transformed
    qualify row_number() over (partition by ims_txn_dt,dstr_cd,dstr_nm,cust_cd,cust_nm,prod_cd,prod_nm,rpt_per_strt_dt,rpt_per_end_dt,ean_num,uom,unit_prc,sls_amt,sls_qty,rtrn_qty,rtrn_amt,ship_cust_nm,cust_cls_grp,cust_sub_cls,prod_spec,itm_agn_nm,ordr_co,rtrn_rsn,sls_ofc_cd,sls_grp_cd,sls_ofc_nm,sls_grp_nm,acc_type,co_cd,sls_rep_cd,sls_rep_nm,doc_dt,doc_type,doc_num,invc_num,remark_desc,gift_qty,sls_bfr_tax_amt,sku_per_box,ctry_cd,crncy_cd  order by crt_dttm,updt_dttm) = 1
)
select * from final

