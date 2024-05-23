with source as(
    select * from {{ source('ntasdl_raw', 'sdl_tw_ims_dstr_prod_map') }}
),
transformed as(
    SELECT SRC.dstr_code as dstr_code,
       SRC.dstr_name as dstr_name,
       SRC.dstr_product_code as dstr_product_code,
       SRC.dstr_product_name as dstr_product_name,
	   SRC.ean_code as ean_code,
       updt_dttm
    FROM source SRC
)
select * from transformed
