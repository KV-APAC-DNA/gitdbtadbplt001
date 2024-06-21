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
        salesman.smname as smname,
        getdate() as CRT_DTTM
        FROM itg_salesmanmaster salesman
        Union All
        Select 
        '-1' as distcode,
        '-1' as smcode,
        'Unknown' as smname,
        current_timestamp() as crt_dttm
),
final as 
(
    select 
    distcode::varchar(100) as distcode,
	smcode::varchar(100) as smcode,
	smname::varchar(100) as smname,
	crt_dttm::timestamp_ntz(9) as crt_dttm
    from trans
)
select * from final