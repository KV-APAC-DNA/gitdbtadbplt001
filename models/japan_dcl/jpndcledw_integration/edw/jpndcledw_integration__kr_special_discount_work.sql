-- not able to load due to permission issue 

with tbusrpram
as
(
    select * from dev_dna_core.snapjpdclitg_integration.tbusrpram
    )

,

c_tbecregularmeisai
as
(
    select * from dev_dna_core.snapjpdclitg_integration.c_tbecregularmeisai
)

,

kr_special_discount_file
as
(
    select * from dev_dna_core.snapjpdclitg_integration.kr_special_discount_file
)

,

c_tbecregularcontract
as
(
    select * from dev_dna_core.jpdclitg_integration.c_tbecregularcontract
)

,

transformed
as
(
    select distinct lpad(usr.diusrid,10,'0') as diusrid
      ,usr.dsname
      ,usr.dsname2
      ,usr.dszip
      ,usr.dspref || usr.dscity || usr.dsaddr as dsaddr
      ,usr.dstatemono
      ,teiki.dsitemid
      ,usr.dsctrlcd
      ,case when usr.dsdat5 = 'pc'    then usr.dsmail
            when usr.dsdat5 = '携帯'  then usr.dsdat1
            when usr.dsdat5 = 'なし'  and  usr.dsmail not like '%dummy.emplex.ne.jp%' then usr.dsmail
            else usr.dsdat1          end as mailaddress
      ,usr.disndflg      --出荷完了メールフラグ
      ,tm.dstodokesei
      ,tm.dstodokemei
      ,tm.dstodokezip
      ,tm.dstodokepref || tm.dstodokecity || tm.dstodokeaddr  as dstodokeaddr
      ,tm.dstodoketatemono
      ,tm.c_diregularcontractid
  from tbusrpram usr
 inner join (select om.c_diusrid
                   ,min(to_char(om.c_dstodokedate,'yyyymmdd')) as c_dstodokedate
                   ,om.dsitemid
                   ,om.c_diregularcontractid
               from c_tbecregularmeisai om --★定期契約明細情報
              where om.c_dicancelflg = 0
                and om.dielimflg     = 0
                and not (om.c_dsordercreatekbn    = '3'   --受注生成区分(3:対象外)
                and      om.c_dscontractchangekbn = '0')  --契約変更区分(0:変更なし)
                and to_char(c_dstodokedate,'yyyymmdd') >= (select distinct cast(extraction_date as varchar) from kr_special_discount_file) --お届け予定日が本日以降
                and exists (select 'x'
                              from c_tbecregularcontract od --★定期契約情報
                             where od.c_diregularcontractid = om.c_diregularcontractid
                               and od.c_dicancelflg = '0'
                               and od.dielimflg     = '0')
                and om.dsitemid in (select distinct dsitemid from kr_special_discount_file) --指定アイテム
              group by om.c_diusrid, om.dsitemid, om.c_diregularcontractid
            ) teiki
    on usr.diusrid = teiki.c_diusrid
 inner join c_tbecregularmeisai tm --★定期契約明細情報
    on teiki.c_diusrid             = tm.c_diusrid
   and teiki.c_dstodokedate        = to_char(tm.c_dstodokedate,'yyyymmdd')
   and teiki.dsitemid              = tm.dsitemid
   and teiki.c_diregularcontractid = tm.c_diregularcontractid
 where usr.dielimflg      = '0' --削除フラグ
   and usr.disecessionflg = '0' --退会者除外
   and usr.dsdat93        <> 'テストユーザ'  --テストユーザフラグ
   and usr.dsdat12        <> 'ブラック' --顧客ステータス
   
)

select COUNT(*) from transformed