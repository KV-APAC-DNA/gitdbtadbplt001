{{
    config
    (
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key= ["dis_code", "sr_code"]
    )
}}
with source as 
(
    select * from {{ source('idnsdl_raw', 'sdl_distributor_ivy_user_master') }}
),
final as
( select * from 
(
    select 
    md_location::varchar(25) as md_location,
	md_code::varchar(15) as md_code,
	md_name::varchar(150) as md_name,
	sd_location::varchar(25) as sd_location,
	sd_code::varchar(15) as sd_code,
	sd_name::varchar(150) as sd_name,
	rbdm_location::varchar(25) as rbdm_location,
	rbdm_code::varchar(15) as rbdm_code,
	rbdm_name::varchar(150) as rbdm_name,
	bdm_location::varchar(25) as bdm_location,
	bdm_code::varchar(15) as bdm_code,
	bdm_name::varchar(150) as bdm_name,
	bdr_location::varchar(25) as bdr_location,
	bdr_code::varchar(15) as bdr_code,
	bdr_name::varchar(150) as bdr_name,
	dis_location::varchar(25) as dis_location,
	dis_code::varchar(15) as dis_code,
	dis_name::varchar(150) as dis_name,
	rsm_code::varchar(15) as rsm_code,
	rsm_name::varchar(150) as rsm_name,
	sup_code::varchar(15) as sup_code,
	sup_name::varchar(150) as sup_name,
	sr_code::varchar(15) as sr_code,
	sr_name::varchar(150) as sr_name,
	cdl_dttm::varchar(200) as cdl_dttm,
	run_id::number(14,0) as run_id,
    row_number() over (partition by sr_code, dis_code order by null) rn,
	file_name::varchar(255) as file_name
    from source)
    where rn=1
)
select * from final