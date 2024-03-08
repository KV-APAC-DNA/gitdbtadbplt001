{{ 
    config(
            materialized="incremental",
            incremental_strategy="delete+insert",
            unique_key=["matl_num_pk"]
        ) 
}}


with sdl_mds_th_product_master as(
    select * from {{ source('thasdl_raw', 'sdl_mds_th_product_master') }}
),
sdl_mds_th_ref_distributor_item_unit as(
    select * from {{ source('thasdl_raw', 'sdl_mds_th_ref_distributor_item_unit') }}
),
b as(
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY barcode ORDER BY createdate DESC NULLS LAST, code DESC NULLS LAST) AS rnk
    FROM sdl_mds_th_product_master
    WHERE
        barcode <> ''
),
a as(
    SELECT * FROM b WHERE rnk = 1
),
transformed as(
    select
        a.code::varchar(50) as matl_num,
        COALESCE(LTRIM(a.code, 0), 'NA')::varchar(50) as matl_num_pk,
        a.name2::varchar(500) as matl_desc,
        groupcode::varchar(50) as mega_brnd,
        typecode::varchar(50) as brnd,
        cateorycode::varchar(50) as base_prod,
        brandcode::varchar(50) as vrnt,
        formatcode::varchar(50) as put_up,
        saleprice2::number(10,2) as gross_prc,
        c.name2::varchar(50) as unit_cd,
        createdate::timestamp_ntz(9) as crt_date,
        barcode::varchar(50) as barcd,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from a
    LEFT JOIN sdl_mds_th_ref_distributor_item_unit AS c
    ON a.defsaleunitcode_code = c.code
)
select * from transformed