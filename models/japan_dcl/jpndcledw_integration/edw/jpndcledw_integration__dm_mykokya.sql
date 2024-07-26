--  add this while deploment 
{{config(
        pre_hook = ["update jp_dcl_itg.mykokya set source_file_date  = null;","update jp_dcl_itg.mykokya_param set source_file_date  = null;"]
    )
}}

with mykokya as
(
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.MYKOKYA
)
, mykokya_param as
( 
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.MYKOKYA_PARAM
),
cim01kokya as
(
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIM01KOKYA
)
,transformed as (
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
	from mykokya m 
	inner join mykokya_param p on m.file_id = p.file_id 
	left join cim01kokya A on m.customer_no = A.webno
	left join cim01kokya B on m.customer_no = B.cardkokyano
    )
	, final as(
select  
FILE_ID::NUMBER(18,0) as FILE_ID,
FILENAME::VARCHAR(100) as FILENAME,
KOKYANO::VARCHAR(20) as KOKYANO,
PURPOSE_TYPE::VARCHAR(30) as PURPOSE_TYPE,
UPLOAD_BY::VARCHAR(50) as UPLOAD_BY,
UPLOAD_DT::VARCHAR(10) as UPLOAD_DT,
UPLOAD_TIME::VARCHAR(8) as UPLOAD_TIME

 from transformed 
 )
 select * from final