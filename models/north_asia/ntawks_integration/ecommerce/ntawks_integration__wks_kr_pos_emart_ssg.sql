with sdl_kr_pos_emart_ssg as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_pos_emart_ssg') }}
    where filename not in (
            select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_pos_emart_ssg__lookup_test') }}
     )
),
itg_sales_store_master as (
    select * from {{ ref('ntaitg_integration__itg_sales_store_master') }}
),
final as (
    select 
        str_nm,
        master.cust_store_cd as str_cd,
        team_nm,
        lrg_classification_nm,
        mid_classification_nm,
        sub_classified_nm,
        offline_ean,
        ean,
        prod_nm,
        pos_dt,
        sellout_qty,
        sellout_amt,
        suppliers,
        product_type,
        crtd_dttm,
        filename
    from sdl_kr_pos_emart_ssg sales, itg_sales_store_master master
    where master.ctry_cd='KR'
    and trim(sales.str_nm)=trim(master.store_nm(+))
)
select * from final