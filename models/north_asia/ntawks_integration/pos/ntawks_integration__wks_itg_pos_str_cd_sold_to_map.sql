{{
    config(
        pre_hook='{{build_itg_pos_str_cd_sold_to_map_temp()}}'
    )
}}
with source as(
    select * from {{ source('ntasdl_raw', 'sdl_mds_kr_pos_store_code_sold_to_map') }}
),
itg_pos_str_cd_sold_to_map_temp as 
(
    select * from {{ source('ntaitg_integration', 'itg_pos_str_cd_sold_to_map_temp') }}
),
final as(
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
        TGT.CRT_DTTM AS TGT_CRT_DTTM,
        current_timestamp()::timestamp_ntz(9) AS upd_dttm,
        CASE
            WHEN TGT.CRT_DTTM IS NULL THEN 'I'
            ELSE 'U'
        END AS CHNG_FLG
    FROM 
    (
        SELECT a.*,
            ROW_NUMBER() OVER (
                PARTITION BY src_sys_cd,
                cust_str_cd ORDER BY sold_to_cd DESC
                ) AS rnk
        FROM source a
        WHERE cust_str_cd IS NOT NULL
            AND cust_str_cd <> ''
    ) src
    LEFT OUTER JOIN (
        SELECT src_sys_cd,
            cust_str_cd,
            CRT_DTTM
        FROM itg_pos_str_cd_sold_to_map_temp
    ) TGT ON SRC.src_sys_cd = TGT.src_sys_cd
    AND SRC.cust_str_cd = TGT.cust_str_cd
    WHERE src.rnk = 1
)
select * from final
