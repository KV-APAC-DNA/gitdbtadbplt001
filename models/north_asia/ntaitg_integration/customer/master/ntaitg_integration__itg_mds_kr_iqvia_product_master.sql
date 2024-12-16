{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["productname","form_description","packsize","chc4_desc"]
    )
}}


with source as (
    select * from {{ source('ntasdl_raw', 'sdl_mds_kr_iqvia_product_master_adftemp') }}
),
final as (
    select
	PRODUCTNAME,
	FORM_DESCRIPTION,
	PACKSIZE,
	CHC4_DESC,
	EAN
    from source
)

select * from final