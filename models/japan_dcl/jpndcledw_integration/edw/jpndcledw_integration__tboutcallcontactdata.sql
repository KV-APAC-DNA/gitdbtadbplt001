with
    tboutcallresult as (

        select * from dev_dna_core.snapjpdcledw_integration.tboutcallresult

    ),

    transformed as (
        select

            'a' as dsctrlflg,
            sr.diusrid as diusrid,
            '11' as dschannel,
            'DNA' || to_char(
                convert_timezone('Asia/Tokyo', current_timestamp()), 'yyyymmdd24hhmiss'
            ) as dscontactno,
            '4' as dssendkbn,
            to_char(
                convert_timezone('Asia/Tokyo', current_timestamp()), 'yyyymmdd'
            ) as dssenddate,
            'アウトコールコンタクト履歴' as dstitle,
            null as dsextdat1,
            null as dsextdat2,
            null as dsextdat3,
            null as dsextdat4,
            null as dsextdat5,
        from tboutcallresult sr
        where sr.excflg = '0'
    ),

    final as (select * from transformed)

select *
from final
