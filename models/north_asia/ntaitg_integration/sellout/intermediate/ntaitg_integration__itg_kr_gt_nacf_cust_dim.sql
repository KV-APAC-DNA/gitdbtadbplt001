{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= " {% if is_incremental() %}
                    delete
                    from {{this}}
                    where (
                            nacf_customer_code,
                            sap_customer_code
                            ) in (
                            select nacf_customer_code,
                                sap_customer_code
                            from na_sdl.sdl_kr_gt_nacf_cust_dim
                            );
                    {% endif %}
                    "
    )
}}
with  SDL_KR_GT_NACF_CUST_DIM as (
    select * from DEV_DNA_LOAD.SNAPNTASDL_RAW.SDL_KR_GT_NACF_CUST_DIM
),
final as (
SELECT 
'KR'::varchar(2) as cntry_cd ,
dstr_nm::varchar(30) as dstr_nm,
nacf_customer_code::varchar(50) as nacf_customer_code,
sap_customer_code::varchar(50) as sap_customer_code,
current_timestamp()::timestamp_ntz(9) as created_dt,
FROM SDL_KR_GT_NACF_CUST_DIM
)
select * from  final
