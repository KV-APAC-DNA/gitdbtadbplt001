{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key = ["year_week"]
        )
}}

--Import CTE
with source as 
(
    select * from {{ source('thasdl_raw', 'sdl_th_mt_watsons_pos_1210_UAT') }}
    where file_name not in (
        select distinct file_name from {{source('thawks_integration','TRATBL_sdl_th_watsons_weekly_pos__null_test')}}
        union all
        select distinct file_name from {{source('thawks_integration','TRATBL_sdl_th_watsons_weekly_pos__duplicate_test')}}
    )
),  

final as 
(
    Select     
        BU_Name :: VARCHAR(100) AS BU_Name,
        Supplier :: VARCHAR(100) AS Supplier,
        Brand_Name :: VARCHAR(200) AS Brand_Name,
        Product_ID :: VARCHAR(100) AS Product_ID,
        Product_Name :: VARCHAR(500) AS Product_Name,
        Category_1 :: VARCHAR(200) AS Category_1,
        Category_2 :: VARCHAR(200) AS Category_2,
        Category_3 :: VARCHAR(200) AS Category_3,
        Category_4 :: VARCHAR(200) AS Category_4,
        Year :: VARCHAR(50) AS Year,
        Year_Month :: VARCHAR(50) AS Year_Month,
        Year_Week :: VARCHAR(50) AS Year_Week,
        Transaction_Date :: VARCHAR(50) AS Transaction_Date,
        Online_Offline :: VARCHAR(50) AS Online_Offline,
        Store_Format :: VARCHAR(200) AS Store_Format,
        Store_Segment :: VARCHAR(200) AS Store_Segment,
        Gross_Sales :: VARCHAR(200) AS Gross_Sales,
        Net_Sales :: VARCHAR(200) AS Net_Sales,
        Sales_Qty :: VARCHAR(200) AS Sales_Qty,
        Barcode :: VARCHAR(200) AS Barcode,
        convert_timezone('Asia/Singapore',current_timestamp)::timestamp_ntz(9) AS crt_dttm,
        convert_timezone('Asia/Singapore',current_timestamp)::timestamp_ntz(9) AS upd_dttm
    from source
)

--Final Select

select * from final            