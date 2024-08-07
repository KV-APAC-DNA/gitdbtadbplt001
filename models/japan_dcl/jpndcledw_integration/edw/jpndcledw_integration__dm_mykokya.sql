 {{ config(
	        pre_hook = ["update {{ ref('jpndclitg_integration__mykokya') }} set source_file_date  = null;",
                        "update {{ ref('jpndclitg_integration__mykokya_param') }} set source_file_date  = null;"]
	    )
}}

with mykokya as (
select * from dev_dna_core.snapjpdclitg_integration.mykokya
),

mykokya_param as (
select * from dev_dna_core.snapjpdclitg_integration.mykokya_param
),

cim01kokya as (
select * from dev_dna_core.snapjpdcledw_integration.cim01kokya
),

transformed as (
select m.file_id,
	m.filename,
	case when lower(p.customer_no_type) = 'webno' then A.kokyano
	when lower(p.customer_no_type) = 'cardkokyano' then B.kokyano
	when lower(p.customer_no_type) = 'kokyano_d' then {{ encryption_1("(lpad(m.customer_no,10,'0'))")}}
	else lpad(m.customer_no,10,'0') end as kokyano,
	p.purpose_type,
	p.upload_by,
	p.upload_dt,
	p.upload_time
	from mykokya as m
	inner join mykokya_param as p on m.file_id = p.file_id
	left join cim01kokya as a on m.customer_no = a.webno
	left join cim01kokya as b on m.customer_no = b.cardkokyano
    ),

final as (
select
file_id::NUMBER(18, 0) as file_id,
filename::VARCHAR(100) as filename,
kokyano::VARCHAR(20) as kokyano,
purpose_type::VARCHAR(30) as purpose_type,
upload_by::VARCHAR(50) as upload_by,
upload_dt::VARCHAR(10) as upload_dt,
upload_time::VARCHAR(8) as upload_time

 from transformed
 )

select * from final


