with
    kr_special_discount_work as (
        select * from dev_dna_core.jpdcledw_integration.kr_special_discount_work
    ),

    kr_special_discount_file as (
        select * from dev_dna_core.jpdclitg_integration.kr_special_discount_file
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
                            dsctrlcd is not null  -- webidあり
                            and mailaddress is not null  -- メールアドレスあり
                            and disndflg = 1
                        then 1
                        else 2
                    end
            end as patternid
        from kr_special_discount_work wrk
        inner join
            kr_special_discount_file sdf on trim(wrk.dsitemid) = trim(sdf.dsitemid)
    )

select *
from transformed
