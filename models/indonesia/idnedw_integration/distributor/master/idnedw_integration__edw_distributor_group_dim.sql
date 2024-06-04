with source as 
(
    select * from {{ ref('idnitg_integration__itg_distributor_group_dim') }}
),
final as 
(
    select 
    dstrbtr_grp_cd::varchar(25) as dstrbtr_grp_cd,
	dstrbtr_grp_nm::varchar(155) as dstrbtr_grp_nm,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final