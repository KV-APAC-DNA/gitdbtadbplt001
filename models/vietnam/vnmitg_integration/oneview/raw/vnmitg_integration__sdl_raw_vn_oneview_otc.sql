{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('vnmsdl_raw','sdl_vn_oneview_otc') }}
    where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_mt_sellout_vinmart__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_mt_sellout_vinmart__duplicate_test')}}
    )
),
final as(
    select 
        plant,
        principalcode,
        principal,
        product,
        productname,
        kunnr,
        name1,
        name2,
        address,
        province,
        zterm,
        kdgrp,
        custgroup,
        region,
        district,
        vbeln,
        billingdate,
        reason,
        qty,
        dgle,
        pernr,
        vat,
        suom,
        custpayto,
        tt,
        nguyengia,
        ttv,
        discount,
        device_code,
        device,
        order_no,
        orginv,
        batch,
        charge,
        contact_name,
        userid,
        billinginst,
        distchannel,
        redinv,
        serial,
        potext,
        expdate,
        ret_so,
        vat_code,
        sodoc_date,
        itemnotes,
        mg1,
        year,
        month,
        channel,
        null as groups,
        file_name,
        run_id,
        crt_dttm 
    from source
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
 {% endif %}
)

select * from final