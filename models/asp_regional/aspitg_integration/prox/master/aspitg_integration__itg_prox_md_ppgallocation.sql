with sdl_PROX_MD_PPGAllocation as(
    select * from {{ source('aspsdl_raw', 'sdl_prox_md_ppgallocation') }}
),
final as(
SELECT 
id::varchar(50) as id,
ppgcode::varchar(50) as ppgcode,
materialcode::varchar(50) as materialcode,
allocationrate::number(18,4) as allocationrate,
status::varchar(50) as status,
applicationid::varchar(50) as applicationid,
createtime::timestamp_ntz(9) as createtime,
createuserid::varchar(50) as createuserid,
lastmodifytime::timestamp_ntz(9) as lastmodifytime,
lastmodifyuserid::varchar(50) as lastmodifyuserid,
batchno::varchar(50) as batchno,
filename::varchar(100) as filename,
run_id::varchar(50) as run_id,
crt_dttm::timestamp_ntz(9) as crt_dttm
FROM sdl_PROX_MD_PPGAllocation
)
select * from final
