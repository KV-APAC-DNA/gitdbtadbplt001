with
    kr_special_discount_work as (
        select * from  {{ ref('jpndcledw_integration__kr_special_discount_work') }}
    ),

    kr_special_discount_file as (
        select * from {{ ref('jpndclitg_integration__kr_special_discount_file') }}
    ),

    transformed as (
        select
            wrk.*,
            case
                when sdf.patternid = 0
                then 0
                else
                    case
                        when
                            rtrim(dsctrlcd) is not null  -- webidあり
                            and rtrim(mailaddress) is not null  -- メールアドレスあり
                            and rtrim(disndflg) = 1
                        then 1
                        else 2
                    end
            end as patternid
        from kr_special_discount_work wrk
        inner join
            kr_special_discount_file sdf on trim(wrk.dsitemid) = trim(sdf.dsitemid)
    )
,
final
as(
select 
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
	dstodokesei::varchar(30) as dstodokesei,
	dstodokemei::varchar(30) as dstodokemei,
	dstodokezip::varchar(15) as dstodokezip,
	dstodokeaddr::varchar(213) as dstodokeaddr,
	dstodoketatemono::varchar(90) as dstodoketatemono,
	c_diregularcontractid::number(38,0) as c_diregularcontractid,
	patternid::number(18,0) as patternid

from transformed
)

select * from final 

    