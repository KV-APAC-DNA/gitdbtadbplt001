{{
    config(
        materialized="incremental",
        incremental_strategy = "delete+insert",
        unique_key=["dstrbtr_id","sls_ord_dt"]
    )
}}

with source as (
    select * from {{ source('myssdl_raw', 'sdl_my_monthly_sellout_sales_fact') }}
),

imier as (
    select * from {{ ref('mysitg_integration__itg_my_ids_exchg_rate') }}
),

logical as (
    select
        dstrbtr_id,
        sls_ord_num,
        to_date(sls_ord_dt,'dd.mm.yyyy') as sls_ord_dt,
        type,
        cust_cd,
        dstrbtr_wh_id,
        item_cd,
        dstrbtr_prod_cd,
        ean_num,
        dstrbtr_prod_desc,
        cast(grs_prc as decimal(20, 4)) as grs_prc,
        cast(qty as decimal(20, 4)) as qty,
        uom,
        cast(qty_pc as decimal(20, 4)) as qty_pc,
        cast(qty_aft_conv as decimal(20, 4)) as qty_aft_conv,
        cast(subtotal_1 as decimal(20, 4)) * coalesce(imier.exchng_rate, 1) as subtotal_1,
        cast(discount as decimal(20, 4)) * coalesce(imier.exchng_rate, 1) as discount,
        cast(subtotal_2 as decimal(20, 4)) * coalesce(imier.exchng_rate, 1) as subtotal_2,
        cast(bottom_line_dscnt as decimal(20, 4)) * coalesce(imier.exchng_rate, 1) as bottom_line_dscnt,
        cast(total_amt_aft_tax as decimal(20, 4)) * coalesce(imier.exchng_rate, 1) as total_amt_aft_tax,
        cast(total_amt_bfr_tax as decimal(20, 4)) * coalesce(imier.exchng_rate, 1) as total_amt_bfr_tax,
        sls_emp,
        custom_field1,
        custom_field2,
        custom_field3 as sap_matl_num,
        filename,
        source.cdl_dttm as cdl_dttm,
        source.curr_dt as crtd_dttm,
        current_timestamp() as updt_dttm
    from source, imier
    where imier.cust_id(+)=source.dstrbtr_id
        and imier.yearmo(+)=replace(substring(to_date(sls_ord_dt,'dd.mm.yyyy'), 0, 7),'-', '')
),

final as (
    select 
        dstrbtr_id::varchar(50) as dstrbtr_id,
        sls_ord_num::varchar(50) as sls_ord_num,
        sls_ord_dt::date as sls_ord_dt,
        type::varchar(20) as type,
        cust_cd::varchar(50) as cust_cd,
        dstrbtr_wh_id::varchar(50) as dstrbtr_wh_id,
        item_cd::varchar(50) as item_cd,
        dstrbtr_prod_cd::varchar(50) as dstrbtr_prod_cd,
        ean_num::varchar(50) as ean_num,
        dstrbtr_prod_desc::varchar(100) as dstrbtr_prod_desc,
        grs_prc::number(20,4) as grs_prc,
        qty::number(20,4) as qty,
        uom::varchar(20) as uom,
        qty_pc::number(20,4) as qty_pc,
        qty_aft_conv::number(20,4) as qty_aft_conv,
        subtotal_1::number(20,4) as subtotal_1,
        discount::number(20,4) as discount,
        subtotal_2::number(20,4) as subtotal_2,
        bottom_line_dscnt::number(20,4) as bottom_line_dscnt,
        total_amt_aft_tax::number(20,4) as total_amt_aft_tax,
        total_amt_bfr_tax::number(20,4) as total_amt_bfr_tax,
        sls_emp::varchar(100) as sls_emp,
        custom_field1::varchar(255) as custom_field1,
        custom_field2::varchar(255) as custom_field2,
        sap_matl_num::varchar(255) as sap_matl_num,
        filename::varchar(255) as filename,
        cdl_dttm::varchar(255) as cdl_dttm,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from logical
)

select * from final