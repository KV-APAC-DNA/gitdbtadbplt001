with source as
(
    select * from {{ ref('hcpwks_integration__wks_hcp360_master_key_brand_temp') }}
),
final as
(
    select brand::varchar(20) as brand,
    master_hcp_key::varchar(32) as master_hcp_key,
    mobile_number::varchar(40) as mobile_phone,
    email::varchar(180) as person_email,
    account_source_id::varchar(18) as account_source_id,
    team_name::varchar(20) as ventasys_team_name,
    v_custid::varchar(20) as ventasys_custid,
    subscriber_key::varchar(100) as subscriber_key
     from source
)
select * from final