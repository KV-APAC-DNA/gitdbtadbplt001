with c_tbecinquirekesai as(
    select * from {{ ref('jpndclitg_integration__c_tbecinquirekesai') }}  
),
c_tbecinquire as(
    select * from {{ ref('jpndclitg_integration__c_tbecinquire') }} 
),
tt01_henpin_riyu as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.tt01_henpin_riyu
),
inquirekesai as (
    select k.c_dikesaiid as c_dikesaiid_before,
        k.c_dikesaiid || 'D' as c_dikesaiid_after,
        k.diinquireid,
        k.c_diinquirekesaiid,
        row_number() over (
            partition by k.c_dikesaiid order by k.c_diinquirekesaiid
            ) as num
    --bgn-add 20200108 d.yamashita ***変更19855(jj連携処理の追加におけるdwhデータの差分抽出実現化)****
    --dna側でデータマートを作成するため廃止
    --,to_number(greatest(
    --       to_number(nvl(to_char(nvl(k.dsren,k.dsprep),'yyyymmddhh24miss'),0))
    --      ,to_number(nvl(to_char(nvl(i.dsren,i.dsprep),'yyyymmddhh24miss'),0))
    --      ,to_number(nvl(to_char(tt01.updatedate,'yyyymmddhh24miss'),0))
    -- ))                                                                         as join_rec_upddate     --結合レコード更新日時(jj連携の差分抽出に使用)
    --end-add 20200108 d.yamashita ***変更19855(jj連携処理の追加におけるdwhデータの差分抽出実現化)****
    from c_tbecinquirekesai k
    join c_tbecinquire i on k.diinquireid = i.diinquireid
    join tt01_henpin_riyu tt01 on k.diinquireid = tt01.diinquireid
        and k.c_diinquirekesaiid = tt01.diinquirekesaiid
        and i.diinquireid = tt01.diinquireid
    where k.c_dikesaiid in (
            select k2.c_dikesaiid
            from c_tbecinquirekesai k2
            where k2.dielimflg = '0'
            group by k2.c_dikesaiid
            having count(1) > 1
            )
        and k.dielimflg = '0'
        and i.dielimflg = '0'
        and nvl(i.c_dshenpinsts, '0') in ('3010', '5020')
),
transformed as(
    select c_dikesaiid_before,
        c_dikesaiid_after,
        diinquireid,
        c_diinquirekesaiid
    --bgn-add 20200108 d.yamashita ***変更19855(jj連携処理の追加におけるdwhデータの差分抽出実現化)****
    --dna側でデータマートを作成するため廃止
    --,join_rec_upddate
    --end-add 20200108 d.yamashita ***変更19855(jj連携処理の追加におけるdwhデータの差分抽出実現化)****
    from inquirekesai
    where num > 1
),
final as(
    select
        c_dikesaiid_before::number(10,0) as c_dikesaiid_before,
        c_dikesaiid_after::varchar(62) as c_dikesaiid_after,
        diinquireid::number(10,0) as diinquireid,
        c_diinquirekesaiid::number(10,0) as c_diinquirekesaiid
    from transformed
)
select * from final