with dm_user_status_tmp as(
    select * from SNAPJPDCLEDW_INTEGRATION.dm_user_status_tmp
),
final as(
    select 
        base::varchar(11) as base,
        kokyano::varchar(60) as kokyano,
        dt::timestamp_ntz(9) as dt,
        status::varchar(8) as status
    from dm_user_status_tmp
)
select * from final