with
t1 as
(select * from {{ source('ntasdl_raw', 'sdl_mds_kr_sales_store_master_1') }})
select * from t1