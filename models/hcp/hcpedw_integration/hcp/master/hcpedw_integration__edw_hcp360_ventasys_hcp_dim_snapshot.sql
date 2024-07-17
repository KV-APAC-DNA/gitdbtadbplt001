with vw_edw_hcp360_ventasys_hcp_dim as
(
    select * from snapindedw_integration.vw_edw_hcp360_ventasys_hcp_dim
),
final as
(
    select hcp_id::varchar(20) as hcp_id,
    hcp_master_id::varchar(50) as hcp_master_id,
    territory_id::varchar(20) as territory_id,
    customer_name::varchar(150) as customer_name,
    customer_type::varchar(20) as customer_type,
    qualification::varchar(50) as qualification,
    speciality::varchar(20) as speciality,
    core_noncore::varchar(20) as core_noncore,
    classification::varchar(20) as classification,
    is_fbm_adopted::varchar(20) as is_fbm_adopted,
    planned_visits_per_month::varchar(10) as planned_visits_per_month,
    cell_phone::varchar(50) as cell_phone,
    phone::varchar(100) as phone,
    email::varchar(100) as email,
    city::varchar(50) as city,
    state::varchar(50) as state,
    is_active::varchar(10) as is_active,
    first_rx_date::date as first_rx_date,
    cust_entered_date::date as cust_entered_date,
    valid_from::timestamp_ntz(9) as valid_from,
    valid_to::timestamp_ntz(9) as valid_to,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
    from vw_edw_hcp360_ventasys_hcp_dim
)
select * from final