{{
    config(
        pre_hook="{{build_itg_ims_dstr_cust_attr_temp()}}"
    )
}}
with sdl_hk_ims_dstr_cust_attr as(
    select * from {{ source('ntasdl_raw', 'sdl_hk_ims_dstr_cust_attr') }}
),
itg_ims_dstr_cust_attr as 
(
    -- select * from {{ source('ntaitg_integration', 'itg_ims_dstr_cust_attr_temp') }}
    select * from ntaitg_integration__itg_ims_dstr_cust_attr_temp
),
final as (
    select src.dstr_code,
        null as dstr_name,
        src.dstr_customer_code,
        src.dstr_customer_name,
        null as dstr_customer_area,
        src.dstr_customer_clsn1,
        src.dstr_customer_clsn2,
        src.dstr_customer_clsn3,
        src.dstr_customer_clsn4,
        src.dstr_customer_clsn5,
        'HK' as ctry_cd,
        tgt.crt_dttm as tgt_crt_dttm,
        updt_dttm,
        case
            when tgt.crt_dttm is null then 'I'
            else 'U'
        end as chng_flg
    from sdl_hk_ims_dstr_cust_attr src
        left outer join (
            select dstr_cust_cd,
                dstr_cust_nm,
                dstr_cd,
                ctry_cd,
                crt_dttm
            from itg_ims_dstr_cust_attr
        ) tgt on src.dstr_customer_code = tgt.dstr_cust_cd
        /*and src.dstr_customer_name=tgt.dstr_cust_nm*/
        and src.dstr_code = tgt.dstr_cd
        and tgt.ctry_cd = 'HK'
)
select * from final