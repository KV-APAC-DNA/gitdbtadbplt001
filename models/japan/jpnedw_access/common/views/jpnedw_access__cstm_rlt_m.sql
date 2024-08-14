with source as(
    select * from {{ ref('jpnedw_integration__cstm_rlt_m') }}
),
transformed as(
    SELECT create_dt as "create_dt"
        ,create_user as "create_user"
        ,update_dt as "update_dt"
        ,update_user as "update_user"
        ,sold_to_cstm as "sold_to_cstm"
        ,ship_to_cstm as "ship_to_cstm"
        ,bill_to_cstm as "bill_to_cstm"
        ,pay_cstm as "pay_cstm"
    FROM source
)
select * from transformed