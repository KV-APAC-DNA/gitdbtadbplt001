with source as (
    select * from {{ source('myssdl_raw','sdl_my_customer_dim') }}
),

final as (
    select
        cust_id::varchar(20) as cust_id,
        cust_nm::varchar(100) as cust_nm,
        dstrbtr_grp_cd::varchar(20) as dstrbtr_grp_cd,
        dstrbtr_grp_nm::varchar(100) as dstrbtr_grp_nm,
        ullage::number(20, 4) as ullage,
        chnl::varchar(20) as chnl,
        territory::varchar(20) as territory,
        retail_env::varchar(20) as retail_env,
        trdng_term_val::number(20, 4) as trdng_term_val,
        rdd_ind::varchar(10) as rdd_ind,
        cdl_dttm::varchar(40) as cdl_dttm,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

select * from final
