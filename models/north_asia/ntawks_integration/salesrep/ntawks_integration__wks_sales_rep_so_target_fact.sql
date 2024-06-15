{{
    config
    (
        materialized ='incremental',
        incremental_strategy = 'append',
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where file_rec_dt = to_date(convert_timezone('UTC', current_timestamp())) and ctry_cd = 'HK' and dstr_cd = '110256';
        {% endif %}"
    )
}}


with source as
(
    select * from {{ source ( 'ntasdl_raw', 'sdl_hk_wingkeung_sales_rep_so_tgt_fact')}}
),

final as
(
    select
        'HK' AS ctry_cd,
        'HKD' AS crncy_cd,
        '110256' AS dstr_cd,        
        jj_mnth_id,
        sls_rep_cd,
        sls_rep_nm,
        case 
            when brand is null
            or trim(brand) = ''
            then 'ALL'
            else brand
            end as brand,
        sls_trgt_val,
        to_date(convert_timezone('UTC', current_timestamp())) as file_rec_dt
    from source
)
select * from final