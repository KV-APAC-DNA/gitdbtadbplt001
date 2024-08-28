with itg_tblpf_clstkm as 
(
    select * from {{ ref('inditg_integration__itg_tblpf_clstkm') }}
), 
final as 
(
    select 
    mon::number(18,0) as month,
	yr::number(18,0) as year,
	distcode::varchar(50) as customer_code,
	prdcode::varchar(50) as product_code,
	sum(clstckqty)::number(12,2) as cl_stck_qty,
	sum(value)::number(18,2) as cl_stck_value,
	sum(nr)::number(18,3) as cl_stck_nr,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm,
    file_name::varchar(255) as file_name
    from itg_tblpf_clstkm
    group by mon,
             yr,
             distcode,
             prdcode,
             file_name
)
select * from final