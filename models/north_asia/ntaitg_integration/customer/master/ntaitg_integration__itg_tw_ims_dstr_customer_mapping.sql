{{
    config
    (
        materialized = 'incremental',
        incremental_strategy = 'append',
        pre_hook = "delete
                    from {{this}} itg_tw_ims_dstr_customer_mapping using dev_dna_load.snapntasdl_raw.sdl_mds_tw_ims_dstr_customer_mapping t3
                    where itg_tw_ims_dstr_customer_mapping.distributors_customer_code = t3.distributors_customer_code
                    and itg_tw_ims_dstr_customer_mapping.distributor_code = t3.distributor_code;"
    )
}}


with source as
(
    -- select * from select * from {{ source('ntasdl_raw', 'sdl_mds_tw_ims_dstr_customer_mapping') }}
    select * from dev_dna_load.snapntasdl_raw.sdl_mds_tw_ims_dstr_customer_mapping
),

final as
(
    select
        distributor_code::varchar(255) as distributor_code,
        distributor_name::varchar(255) as distributor_name,
        distributors_customer_code::varchar(255) as distributors_customer_code,
        distributors_customer_name::varchar(255) as distributors_customer_name,
        store_type::varchar(255) as store_type,
        hq::VARCHAR(20) as hq,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm  
    from source         
)

select * from final