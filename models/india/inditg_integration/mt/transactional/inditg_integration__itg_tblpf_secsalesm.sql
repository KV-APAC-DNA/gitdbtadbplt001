{{
    config(
        materialized='incremental',
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where (runmm,runyr) in (select distinct runmm,runyr
                                    from {{ ref('inditg_integration__itg_tblpf_secsalesm_wrk') }}
                                    where upper(src) = 'DS')
                                    and   upper(src) = 'DS';
        delete from {{this}} where (runmm,runyr) in (select distinct runmm,runyr
                                    from {{ ref('inditg_integration__itg_tblpf_secsalesm_wrk') }}
                                    where upper(src) = 'SR')
                                    and   upper(src) = 'SR';
        delete from {{this}} where (runmm,runyr) in (select distinct runmm,runyr
                                    from {{ ref('inditg_integration__itg_tblpf_secsalesm_wrk') }}
                                    where upper(src) = 'SNS')
                                    and   upper(src) = 'SNS';
                    {% endif %}"
    )
}}
with itg_tblpf_secsalesm_wrk as 
(
    select * from {{ ref('inditg_integration__itg_tblpf_secsalesm_wrk') }}
),
final as 
(
    select 
	serno::number(38,0) as serno,
	mon::number(18,0) as mon,
	yr::number(18,0) as yr,
	distcode::varchar(50) as distcode,
	prdcode::varchar(50) as prdcode,
	coalesce(nr,0)::number(12,4) as nr,
	coalesce(prdqty,0)::number(16,4) as prdqty,
	coalesce(prdgrossamt,0)::number(16,4) as prdgrossamt,
	coalesce(ptrvalue,0)::number(16,4) as ptrvalue,
	src::varchar(5) as src,
	coalesce(prdnrvalue,0)::number(18,3) as prdnrvalue,
	coalesce(lpvalue,0)::number(18,3) as lpvalue,
	coalesce(trinqty,0)::number(16,3) as trinqty,
	coalesce(troutqty,0)::number(16,3) as troutqty,
	coalesce(trinval,0)::number(16,3) as trinval,
	coalesce(troutval,0)::number(16,3) as troutval,
	coalesce(nonwaveopenqty,0)::number(16,3) as nonwaveopenqty,
	runmm::number(18,0) as runmm,
	runyr::number(18,0) as runyr,
	iscubeprocess::varchar(1) as iscubeprocess,
	coalesce(pricelistlp,0)::number(16,3) as pricelistlp,
	coalesce(pricelistptr,0)::number(16,3) as pricelistptr,
	coalesce(prdqty_new,0)::number(16,4) as prdqty_new,
	coalesce(prdnrvalue_new,0)::number(16,4) as prdnrvalue_new,
	coalesce(ptrvalue_new,0)::number(16,4) as ptrvalue_new,
	coalesce(dbrestore_nr_prev,0)::number(12,4) as dbrestore_nr_prev,
	coalesce(dbrestore_prdqty_prev,0)::number(16,4) as dbrestore_prdqty_prev,
	coalesce(dbrestore_prdgrossamt_prev,0)::number(16,4) as dbrestore_prdgrossamt_prev,
	coalesce(dbrestore_prdnrvalue_prev,0)::number(16,4) as dbrestore_prdnrvalue_prev,
	coalesce(dbrestore_ptrvalue_prev,0)::number(16,4) as dbrestore_ptrvalue_prev,
	coalesce(dbrestore_nr_current,0)::number(12,4) as dbrestore_nr_current,
	coalesce(dbrestore_prdqty_current,0)::number(16,4) as dbrestore_prdqty_current,
	coalesce(dbrestore_prdgrossamt_current,0)::number(16,4) as dbrestore_prdgrossamt_current,
	coalesce(dbrestore_prdnrvalue_current,0)::number(16,4) as dbrestore_prdnrvalue_current,
	coalesce(dbrestore_ptrvalue_current,0)::number(16,4) as dbrestore_ptrvalue_current,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
    from itg_tblpf_secsalesm_wrk
)
select * from final