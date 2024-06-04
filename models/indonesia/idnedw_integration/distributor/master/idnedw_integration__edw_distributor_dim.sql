with source as 
(
    select * from {{ ref('idnitg_integration__itg_distributor_dim') }}
),
itg_distributor_province_dim as 
(
    select * from {{ ref('idnitg_integration__itg_distributor_province_dim') }}
),
itg_distributor_group_dim as 
(
    select * from {{ ref('idnitg_integration__itg_distributor_group_dim') }}
),
final as 
(
    select 
    idd.dstrbtr_grp_cd::varchar(20) as dstrbtr_grp_cd,
	idd.dstrbtr_id::varchar(20) as dstrbtr_id,
	idd.jj_sap_dstrbtr_id::varchar(20) as jj_sap_dstrbtr_id,
	idd.jj_sap_dstrbtr_nm::varchar(50) as jj_sap_dstrbtr_nm,
	idd.city::varchar(100) as city,
	idd.area::varchar(50) as area,
	idd.region::varchar(50) as region,
	idd.bdm_nm::varchar(50) as bdm_nm,
	idd.rbm_nm::varchar(50) as rbm_nm,
	idd.status::varchar(50) as status,
	idd.lead_tm::number(18,0) as lead_tm,
	idd.prvnce_id::varchar(50) as prvnce_id,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm,
	current_timestamp()::timestamp_ntz(9) as uptd_dttm,
	dstrbtr_grp_nm::varchar(255) as dstrbtr_grp_nm,
	prov_nm::varchar(255) as prov_nm,
	effective_from::varchar(10) as effective_from,
	effective_to::varchar(10) as effective_to
from source as idd,
itg_distributor_province_dim as idpd,
itg_distributor_group_dim as idgd
where idd.dstrbtr_grp_cd=idgd.dstrbtr_grp_cd(+)
and idd.prvnce_id=idpd.prov_id(+)
)
select * from final