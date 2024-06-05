with source as
(
    select * from {{ ref('idnitg_integration__itg_distributor_channel_dim') }}
),
final as 
(
    select 
    dstrbtr_grp_cd::varchar(75) as dstrbtr_grp_cd,
	dstrbtr_chnl_type_id::varchar(75) as dstrbtr_chnl_type_id,
	dstrbtr_chnl_type_nm::varchar(75) as dstrbtr_chnl_type_nm,
	jnj_chnl_type_id::varchar(75) as jnj_chnl_type_id,
	jnj_chnl_type_nm::varchar(50) as jnj_chnl_type_nm,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm,
	effective_from::varchar(10) as effective_from,
	effective_to::varchar(10) as effective_to
    from source
)
select * from final