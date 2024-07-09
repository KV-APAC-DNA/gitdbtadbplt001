with 
itg_tbl_schemewise_apno as 
(
    select * from {{ ref('inditg_integration__itg_tbl_schemewise_apno') }}
),
itg_scheme_header as 
(
    select * from {{ ref('inditg_integration__itg_scheme_header') }}
),
trans as 
(
    (
        select
        z.schid,
        z.schcode,
        z.claimable,
        z.cmpschcode,
        z.schvalidfrom,
        z.schvalidtill,
        z.schstatus,
        z.schdsc,
        coalesce(z.a1,'NA') as a1,
        z.a2,
        z.a3,
        z.a4
        from
        (
        select
        a.schid as schid,
        a.schcode as schcode,
        a.claimable as claimable,
        a.cmpschcode as cmpschcode,
        a.schvalidfrom as schvalidfrom,
        a.schvalidtill as schvalidtill,
        a.schstatus as schstatus,
        a.schdsc as schdsc,
        case when b.schcategorytype1code is null or b.schcategorytype1code='' then substring(b.apno,1,3)
        else b.schcategorytype1code end as a1,
        case when b.schcategorytype2code is null then 'NA'
        when b.schcategorytype2code = '' then 'NA'
        else b.schcategorytype2code end as a2,
        current_timestamp() as a3,
        current_timestamp() as a4
        from itg_scheme_header a
        left outer join itg_tbl_schemewise_apno b on a.schid = b.schid
        ) z )
),
final as 
(
    select 
    schid::number(18,0) as scheme_id,
	schcode::varchar(20) as scheme_code,
	claimable::number(38,0) as claimable,
	cmpschcode::varchar(20) as company_scheme_code,
	schvalidfrom::timestamp_ntz(9) as valid_from,
	schvalidtill::timestamp_ntz(9) as valid_till,
	schstatus::number(38,0) as scheme_status,
	schdsc::varchar(100) as scheme_desc,
	a1::varchar(50) as sch_cat_type1,
	a2::varchar(50) as sch_cat_type2,
	a3::timestamp_ntz(9) as crt_dttm,
	a4::timestamp_ntz(9) as updt_dttm
    from trans
)
select * from final
