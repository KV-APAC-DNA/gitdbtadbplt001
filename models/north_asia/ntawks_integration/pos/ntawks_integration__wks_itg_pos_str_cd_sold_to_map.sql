with source as(
    select * from {{ source('ntasdl_raw', 'sdl_mds_kr_pos_store_code_sold_to_map') }}
),
transformed as(
    select 
        src.name,
        src.seqid,
        src.str_nm,
        src.src_sys_cd,
        CASE 
            WHEN src.src_sys_cd = 'E mart'
                THEN 'Emart'
            WHEN src.src_sys_cd = 'E Land'
                THEN 'E-Land Retail'
            WHEN src.src_sys_cd = 'Lotte Super'
                THEN 'Lotte Chain Super'
            WHEN src.src_sys_cd = 'GS Super'
                THEN 'GS Chain Super'
            ELSE src.src_sys_cd
            END AS conv_sys_cd,
        src.str_type,
        src.cust_str_cd,
        src.sold_to_cd,
        current_timestamp()::timestamp_ntz(9) AS upd_dttm,
     FROM (
        SELECT a.*,
            ROW_NUMBER() OVER (
                PARTITION BY src_sys_cd,
                cust_str_cd ORDER BY sold_to_cd DESC
                ) AS rnk
        FROM source a
        WHERE cust_str_cd IS NOT NULL
            AND cust_str_cd <> ''
        ) src
)
select * from transformed
