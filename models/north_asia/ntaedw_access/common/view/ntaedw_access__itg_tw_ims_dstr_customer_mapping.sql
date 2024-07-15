with source as (
    select * from {{ ref('ntaitg_integration__itg_tw_ims_dstr_customer_mapping') }}
),
final as (
    select
        distributor_code as "distributor_code",
        distributor_name as "distributor_name",
        distributors_customer_code as "distributors_customer_code",
        distributors_customer_name as "distributors_customer_name",
        store_type as "store_type",
        hq as "hq",
        crt_dttm as "crt_dttm",
        updt_dttm as "updt_dttm"
    from source
)
select * from final