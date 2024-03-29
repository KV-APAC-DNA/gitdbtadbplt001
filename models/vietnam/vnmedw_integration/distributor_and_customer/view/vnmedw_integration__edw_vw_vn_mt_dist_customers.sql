with source as(
    select * from {{ ref('vnmitg_integration__itg_vn_mt_dksh_cust_master') }}
),
transformed as(
    select 
        cust.code
        ,cust.name
        ,cust.address
        ,upper((cust.sub_channel_name)::text) as sub_channel
        ,upper((cust.group_account_name)::text) as group_account
        ,upper((cust.account_name)::text) as "account"
        ,upper((cust.region_name)::text) as "region"
        ,upper((cust.province_name)::text) as province
        ,upper((cust.retail_environment)::text) as retail_environment
    from source cust
    where ((cust.active)::text = ('Y'::character varying)::text)
)
select * from transformed

