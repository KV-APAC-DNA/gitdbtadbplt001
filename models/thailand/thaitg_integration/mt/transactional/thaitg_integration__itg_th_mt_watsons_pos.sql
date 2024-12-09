{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key = ["product_id","year_month","year_week","store_format","store_segment"]
        )
}}

--Import CTE
with source as 
(
    select * from {{ source('thasdl_raw', 'sdl_th_mt_watsons_pos') }}
      where file_name not in (
        select distinct file_name from {{source('thawks_integration','TRATBL_SDL_TH_WATSONS_WEEKLY_POS__NULL_TEST')}}
        union all
        select distinct file_name from {{source('thawks_integration','TRATBL_SDL_TH_WATSONS_WEEKLY_POS__DUPLICATE_TEST')}}
    )
),  

final as 
(
    Select     
        BU_Name VARCHAR(100),
        Supplier VARCHAR(100),
        Brand_Name VARCHAR(200),
        Product_ID VARCHAR(100),
        Product_Name VARCHAR(500),
        Category_1 VARCHAR(200),
        Category_2 VARCHAR(200),
        Category_3 VARCHAR(200),
        Category_4 VARCHAR(200),
        Year VARCHAR(50),
        Year_Month VARCHAR(50),
        Year_Week VARCHAR(50),
        Transaction_Date VARCHAR(50),
        Online_Offline VARCHAR(50),
        Store_Format VARCHAR(200),
        Store_Segment VARCHAR(200),
        Gross_Sales VARCHAR(200),
        Net_Sales VARCHAR(200),
        Sales_Qty VARCHAR(200),
        Barcode VARCHAR(200),
        convert_timezone('Asia/Singapore',current_timestamp)::timestamp_ntz(9) AS crt_dttm,
        convert_timezone('Asia/Singapore',current_timestamp)::timestamp_ntz(9) AS upd_dttm
    from source
)

--Final Select

select * from final            