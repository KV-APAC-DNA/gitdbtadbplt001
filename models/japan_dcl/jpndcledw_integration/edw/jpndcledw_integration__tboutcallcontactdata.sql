-- snowflake table needs to be updated 

with tboutcallresult
as
(

    select * from dev_dna_core.snapjpdcledw_integration.tboutcallresult

)
,


transformed 
as
(
    select 

    'a' as dsctrlflg,
    sr.diusrid as diusrid,
    '11' as dschannel,
    'DNA' || to_char(convert_timezone('Asia/Tokyo',current_timestamp()),'yyyymmdd24hhmiss') as dscontactno,
    '4' as dssendkbn,
    to_char(convert_timezone('Asia/Tokyo',current_timestamp()),'yyyymmdd') as dssenddate,
    'アウトコールコンタクト履歴' as dstitle,
    NULL as dsextdat1,
    NULL as dsextdat2,
    NULL as dsextdat3,
    NULL as dsextdat4,
    NULL as dsextdat5,
    from tboutcallresult sr
    where  sr.excflg = '0'
)
,

final as
(

    select * from transformed

)

select * from final
 
