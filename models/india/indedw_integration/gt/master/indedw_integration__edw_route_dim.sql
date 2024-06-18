with 
itg_salesmanmaster as 
(
    select * from {{ ref('inditg_integration__itg_salesmanmaster') }}
),
trans as 
(
    SELECT distinct
        salesman.distcode as distcode,
        salesman.smcode as smcode,
        salesman.rmcode as rmcode,
        salesman.smname as smname,
        salesman.rmname as rmname,
        current_timestamp()::timestamp_ntz(9) as CRT_DTTM
        FROM  itg_salesmanmaster salesman
        Union All
        Select 
        '-1' as distcode,
        '-1' as smcode,
        '-1' as rmcode,
        'Unknown' as smname,
        'Unknown' as rmname,
        current_timestamp()::timestamp_ntz(9) as CRT_DTTM
),
final as 
(
    select 
    distcode::varchar(100) as distcode,
	smcode::varchar(100) as smcode,
	rmcode::varchar(100) as rmcode,
	smname::varchar(100) as smname,
	rmname::varchar(100) as rmname,
	crt_dttm::timestamp_ntz(9) as crt_dttm
    from trans
)
select * from final