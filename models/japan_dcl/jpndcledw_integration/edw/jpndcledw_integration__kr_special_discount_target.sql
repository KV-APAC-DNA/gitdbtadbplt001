with
    kr_special_discount_select as (
        select * from dev_dna_core.jpdcledw_integration.kr_special_discount_select
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

    final as (select * from transformed)

select *
from final
