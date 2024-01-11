
with source as (
    select * from {{ ref('aspwks_integration__wks_edw_list_price') }}
),
final as(
    select
    sls_org::VARCHAR(50) as sls_org,
    material::VARCHAR(50) as material,
    cond_rec_no::VARCHAR(100) as cond_rec_no,
    matl_grp::VARCHAR(100) as matl_grp,
    valid_to::VARCHAR(20) as valid_to,
    knart::VARCHAR(20) as knart,
    dt_from::VARCHAR(20) as dt_from,
    amount::NUMBER(20,4) as amount,
    currency::VARCHAR(20) as currency,
    unit::VARCHAR(20) as unit,
    record_mode::VARCHAR(100) as record_mode,
    comp_cd::VARCHAR(100) as comp_cd,
    price_unit::VARCHAR(50) as price_unit,
    zcurrfpa::VARCHAR(20) as zcurrfpa,
    cdl_dttm::VARCHAR(255) as cdl_dttm,
    current_timestamp::TIMESTAMP_NTZ(9) as crtd_dttm,
    current_timestamp::TIMESTAMP_NTZ(9) as updt_dttm
    from source 
)


select * from final
