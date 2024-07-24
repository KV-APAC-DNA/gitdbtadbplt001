
select count(*) from dev_dna_core.jpdcledw_integration.ocl_exclude_file_id 
union
select count(*) from jpndcledw_integration__ocl_exclude_file_id

select * from (
select 'missing in snap', * from jpndcledw_integration__tboutcallcontactdata
except
select 'missing in snap',* from DEV_DNA_CORE.jpdcledw_integration.tboutcallcontactdata
union all
select 'missing in dbt', * from DEV_DNA_CORE.jpdcledw_integration.tboutcallcontactdata
except
select 'missing in dbt', * from jpndcledw_integration__tboutcallcontactdata
) order by 3,2,1