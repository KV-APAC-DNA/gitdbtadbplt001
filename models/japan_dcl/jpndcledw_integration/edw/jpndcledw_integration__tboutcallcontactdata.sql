with
    tboutcallresult as (

        select * from {{ ref('jpndcledw_integration__tboutcallresult') }}

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

    final as (select 
    dsctrlflg:: varchar(1) as dsctrlflg,
	diusrid:: varchar(10)  as diusrid,
	dschannel:: varchar(2) as dschannel,
	dscontactno:: varchar(20) as dscontactno,
	dssendkbn:: varchar(1) as dssendkbn,
	dssenddate:: varchar(8) as dssenddate,
	dstitle:: varchar(300) as dstitle,
	dsextdat1:: varchar(1) as dsextdat1,
	dsextdat2:: varchar(1) as dsextdat2,
	dsextdat3:: varchar(1) as dsextdat3,
	dsextdat4:: varchar(1) as dsextdat4,
	dsextdat5:: varchar(1) as dsextdat5
     from transformed)

select *
from final

    
