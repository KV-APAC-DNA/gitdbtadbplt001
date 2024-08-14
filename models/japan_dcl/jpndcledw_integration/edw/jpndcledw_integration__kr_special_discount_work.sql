with  tbusrpram as 
    (
        select * from {{ ref('jpndclitg_integration__tbusrpram') }}
        ),

    c_tbecregularmeisai as (
        select * from  {{ ref('jpndclitg_integration__c_tbecregularmeisai') }}
    ),

    kr_special_discount_file as (
        select * from  {{ ref('jpndclitg_integration__kr_special_discount_file') }}

    ),

    c_tbecregularcontract as (
        select * from  {{ ref('jpndclitg_integration__c_tbecregularcontract') }}
    ),

    transformed as (
        select distinct
            lpad(usr.diusrid, 10, '0') as diusrid,
            usr.dsname,
            usr.dsname2,
            usr.dszip,
            usr.dspref || usr.dscity || usr.dsaddr as dsaddr,
            usr.dstatemono,
            teiki.dsitemid,
            usr.dsctrlcd,
            case
                when lower(usr.dsdat5) = 'pc'
                then usr.dsmail
                when usr.dsdat5 = '携帯'
                then usr.dsdat1
                when usr.dsdat5 = 'なし' and usr.dsmail not like '%dummy.emplex.ne.jp%'
                then usr.dsmail
                else usr.dsdat1
            end as mailaddress,
            usr.disndflg,  -- 出荷完了メールフラグ
            tm.dstodokesei,
            tm.dstodokemei,
            tm.dstodokezip,
            tm.dstodokepref || tm.dstodokecity || tm.dstodokeaddr as dstodokeaddr,
            tm.dstodoketatemono,
            tm.c_diregularcontractid
        from tbusrpram usr
        inner join
            (
                select
                    om.c_diusrid,
                    min(to_char(om.c_dstodokedate, 'yyyymmdd')) as c_dstodokedate,
                    om.dsitemid,
                    om.c_diregularcontractid
                from c_tbecregularmeisai om  -- ★定期契約明細情報
                where
                    om.c_dicancelflg = 0
                    and om.dielimflg = 0
                    and not (
                        om.c_dsordercreatekbn = '3'  -- 受注生成区分(3:対象外)
                        and om.c_dscontractchangekbn = '0'
                    )  -- 契約変更区分(0:変更なし)
                    and to_char(c_dstodokedate, 'yyyymmdd') >= (
                        select distinct cast(extraction_date as varchar)
                        from kr_special_discount_file
                    )  -- お届け予定日が本日以降
                    and exists (
                        select 'x'
                        from c_tbecregularcontract od  -- ★定期契約情報
                        where
                            od.c_diregularcontractid = om.c_diregularcontractid
                            and od.c_dicancelflg = '0'
                            and od.dielimflg = '0'
                    )
                    and om.dsitemid
                    in (select distinct dsitemid from kr_special_discount_file)  -- 指定アイテム
                group by om.c_diusrid, om.dsitemid, om.c_diregularcontractid
            ) teiki
            on usr.diusrid = teiki.c_diusrid
        inner join
            c_tbecregularmeisai tm  -- ★定期契約明細情報
            on rtrim(teiki.c_diusrid) = rtrim(tm.c_diusrid)
            and rtrim(teiki.c_dstodokedate) = to_char((tm.c_dstodokedate), 'yyyymmdd')
            and rtrim(teiki.dsitemid) = rtrim(tm.dsitemid)
            and rtrim(teiki.c_diregularcontractid) = rtrim(tm.c_diregularcontractid)
        where
            rtrim(usr.dielimflg) = '0'  -- 削除フラグ
            and rtrim(usr.disecessionflg) = '0'  -- 退会者除外
            and rtrim(usr.dsdat93) <> 'テストユーザ'  -- テストユーザフラグ
            and rtrim(usr.dsdat12) <> 'ブラック'  -- 顧客ステータス

    )
,

final as
(select 
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
	c_diregularcontractid::number(38,0) c_diregularcontractid
from transformed
)

select * from final 

    
    
	 
