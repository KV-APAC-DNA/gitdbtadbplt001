with source as (
    select * from {{ ref('idnedw_integration__edw_vw_order_sell_in_analysis') }}
),
final as (
    select
        datasrc::varchar(20) as datasrc,
        to_date(invoice_dt) as invoice_dt,
        bill_doc::varchar(75) as bill_doc,
        jj_year::number(18,0) as jj_year,
        jj_qrtr::varchar(24) as jj_qrtr,
        jj_mnth_id::varchar(24) as jj_mnth_id,
        jj_mnth::varchar(25) as jj_mnth,
        jj_mnth_desc::varchar(20) as jj_mnth_desc,
        jj_mnth_no::number(18,0) as jj_mnth_no,
        jj_mnth_long::varchar(10) as jj_mnth_long,
        dstrbtr_grp_cd::varchar(500) as dstrbtr_grp_cd,
        dstrbtr_grp_nm::varchar(500) as dstrbtr_grp_nm,
        jj_sap_dstrbtr_id::varchar(50) as jj_sap_dstrbtr_id,
        jj_sap_dstrbtr_nm::varchar(100) as jj_sap_dstrbtr_nm,
        dstrbtr_cd_nm::varchar(100) as dstrbtr_cd_nm,
        area::varchar(50) as area,
        region::varchar(100) as region,
        bdm_nm::varchar(100) as bdm_nm,
        rbm_nm::varchar(100) as rbm_nm,
        dstrbtr_status::varchar(50) as dstrbtr_status,
        jj_sap_prod_id::varchar(50) as jj_sap_prod_id,
        jj_sap_prod_desc::varchar(100) as jj_sap_prod_desc,
        jj_sap_upgrd_prod_id::varchar(50) as jj_sap_upgrd_prod_id,
        jj_sap_upgrd_prod_desc::varchar(100) as jj_sap_upgrd_prod_desc,
        jj_sap_cd_mp_prod_id::varchar(50) as jj_sap_cd_mp_prod_id,
        jj_sap_cd_mp_prod_desc::varchar(100) as jj_sap_cd_mp_prod_desc,
        sap_prod_code_name::varchar(200) as sap_prod_code_name,
        franchise::varchar(750) as franchise,
        brand::varchar(750) as brand,
        sku_grp_or_variant::varchar(100) as sku_grp_or_variant,
        sku_grp1_or_variant1::varchar(100) as sku_grp1_or_variant1,
        sku_grp2_or_variant2::varchar(100) as sku_grp2_or_variant2,
        sku_grp3_or_variant3::varchar(100) as sku_grp3_or_variant3,
        prod_status::varchar(50) as prod_status,
        sellin_qty::number(38,4) as sellin_qty,
        sellin_val::number(38,4) as sellin_val,
        gross_sellin_val::number(38,4) as gross_sellin_val,
        order_net::number(38,4) as order_net,
        order_qty::number(38,4) as order_qty,
        order_gross::number(38,4) as order_gross,
        nts_order_val::number(38,4) as nts_order_val,
        to_date(order_dt) as order_dt,
        order_doc::varchar(20) as order_doc,
        target_niv::number(25,7) as target_niv,
        target_hna::number(25,7) as target_hna
    from source
)
select * from final