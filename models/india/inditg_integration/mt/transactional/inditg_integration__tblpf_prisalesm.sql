{{
    config(
        materialized='incremental',
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}}
        where (mon, yr) in (
                select distinct mon,yr from 
        {{ ref('inditg_integration__tblpf_prisalesm_wrk') }});
        {% endif %}"
    )
}}
with tblpf_prisalesm_wrk as 
(
    select * from {{ ref('inditg_integration__tblpf_prisalesm_wrk') }}
),
final as 
(
    select 
	serno::number(38,0) as serno,
	mon::number(18,0) as mon,
	yr::number(18,0) as yr,
	distcode::varchar(50) as distcode,
	prdcode::varchar(50) as prdcode,
	billtype::varchar(50) as billtype,
	qty::number(16,4) as qty,
	value::number(16,4) as value,
	nr::number(12,4) as nr,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm,
    file_name:: varchar(255) as file_name
    from tblpf_prisalesm_wrk
)
select * from final