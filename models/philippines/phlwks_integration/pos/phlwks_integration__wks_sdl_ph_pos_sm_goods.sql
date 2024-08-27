with source as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_sm_goods') }}
    where file_name not in (
        select distinct file_name from {{SOURCE('phlwks_integration','tratbl_sdl_ph_pos_sm_goods__null_test')}}
        union all
        select distinct file_name from {{SOURCE('phlwks_integration','tratbl_sdl_ph_pos_sm_goods__lookup_sm_CUST_CD')}}
        union all
        select distinct file_name from {{SOURCE('phlwks_integration','tratbl_sdl_ph_pos_sm_goods__lookup_sm_SAP_ITEM_CD')}}
        union all
        select distinct file_name from {{SOURCE('phlwks_integration','tratbl_sdl_ph_pos_sm_goods__lookup_sm_JNJ_PC_PER_CUST_UNIT')}}
        union all
        select distinct file_name from {{SOURCE('phlwks_integration','tratbl_sdl_ph_pos_sm_goods__lookup_sm_LST_PRICE_UNIT')}}
        union all
        select distinct file_name from {{SOURCE('phlwks_integration','tratbl_sdl_ph_pos_sm_goods__lookup_sm_PL_JJ_MNTH_ID')}}
    )
),
final as 
(
    select 
        business_name,
        title,
        date,
        vendor_code,
        vendor_name,
        receipt_date,
        terms_and_discount,
        site_code,
        site_name,
        ship_to,
        gr_number,
        po_number,
        cancel_date,
        total_articles,
        article_number,
        article_description,
        upc,
        uom,
        received_qty,
        remarks,
        file_name,
        crt_dttm
    from source
)
select * from final