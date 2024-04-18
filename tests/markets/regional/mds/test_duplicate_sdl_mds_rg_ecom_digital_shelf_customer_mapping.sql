
with grouped_by as(
    select 
        'Duplicate records present' AS failure_reason,
            coalesce((trim(market)),'NA') as market,
            coalesce((trim(online_store)),'NA') as online_store
    from {{ env_var('DBT_ENV_LOAD_DB') }}.aspsdl_raw.sdl_mds_rg_ecom_digital_shelf_customer_mapping
    group by 
    coalesce((trim(market)),'NA'),
    coalesce((trim(online_store)),'NA')
    having  
    count(*) >1  
)
select * from grouped_by