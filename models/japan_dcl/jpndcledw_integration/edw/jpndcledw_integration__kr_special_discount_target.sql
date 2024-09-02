with
    kr_special_discount_select as (
        select * from {{ ref('jpndcledw_integration__kr_special_discount_select') }}
    ),
    transformed as (
        select
            lpad(diusrid, 10, '0') as diusrid,
            dsname,
            dsname2,
            dszip,
            dsaddr,
            dstatemono,
            dsitemid,
            dsctrlcd,
            mailaddress,
            disndflg,
            patternid,
            c_diregularcontractid
        from kr_special_discount_select
        group by
            diusrid,
            dsname,
            dsname2,
            dszip,
            dsaddr,
            dstatemono,
            dsitemid,
            dsctrlcd,
            mailaddress,
            disndflg,
            patternid,
            c_diregularcontractid
        order by diusrid
    ),

    final as (select 
    diusrid::varchar(40) as diusrid,
	dsname::varchar(30) as dsname,
	dsname2::varchar(30) as dsname2,
	dszip::varchar(15) as dszip,
	dsaddr::varchar(213) as dsaddr,
	dstatemono::varchar(90) as dstatemono,
	dsitemid::varchar(45) as dsitemid,
	dsctrlcd::varchar(24) as dsctrlcd,
	mailaddress::varchar(6000) as mailaddress,
	disndflg::varchar(1) as disndflg,
	patternid::number(18,0) as patternid,
	c_diregularcontractid::number(38,0) as c_diregularcontractid

    from transformed)

select *
from final


    