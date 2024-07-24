{{
    config(
        materialized = "incremental",
        incremental_strategy = "append"
    )
}}


with OCL_in_userlist_v
as
(
    select * from dev_dna_core.jpdcledw_integration.OCL_in_userlist_v
)



,

transformed
as
(
select 
	file_id,
	filename ,
	CONVERT_TIMEZONE('Asia/Tokyo', sysdate()) AS insert_date
FROM
	OCL_in_userlist_v
)
,

final
as
(
select * from transformed
)

select * from final 