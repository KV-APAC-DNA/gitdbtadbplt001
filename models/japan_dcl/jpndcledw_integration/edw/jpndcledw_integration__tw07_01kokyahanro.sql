with tw07_01kokyahanro_w as (
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.tw07_01kokyahanro_w
),
tt01kokyastsh_mv as (
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.tt01kokyastsh_mv
),
tm55_teisuchi as (
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.tm55_teisuchi
),
tw07_01kokyahanro_w_null as (
    select * from tw07_01kokyahanro_w where hanrocode is null
),
combined_source as (
    select
	    t07.kokyano
        ,t01.hanrocode
        ,t01.syohanrobunname as syobuncode
        ,t01.chuhanrobunname as chubuncode
        ,t01.daihanrobunname as daibuncode
        ,max(t01.juchdate) as lastjuchdate
        ,max(t01.saleno) as saleno		
	from tw07_01kokyahanro_w_null t07
	left join tt01kokyastsh_mv   t01 on t07.kokyano=t01.kokyano
	cross join tm55_teisuchi tm55
	where tm55.kubunid = '120'
	group by 
        t07.kokyano,
        t01.hanrocode,
        t01.syohanrobunname,
        t01.chuhanrobunname,
        t01.daihanrobunname
),
transformed as (
    select
        kokyano
        ,ifnull(hanrocode,'不明') as hanrocode  
        ,ifnull(syobuncode,'不明') as syobuncode
        ,ifnull(chubuncode,'不明') as chubuncode
        ,ifnull(daibuncode,'不明') as daibuncode
    from 
        combined_source
    qualify row_number() over(partition by kokyano order by lastjuchdate desc, saleno desc) = 1

    union 

    select 
        kokyano
        ,hanrocode
        ,syobuncode
        ,chubuncode
        ,daibuncode
    from 
        tw07_01kokyahanro_w
    where
        hanrocode is not null
),
final as (
    select
        kokyano::varchar(15) as kokyano,
        hanrocode::varchar(60) as hanrocode,
        syobuncode::varchar(60) as daibuncode,  
        chubuncode::varchar(60) as chubuncode,
        daibuncode::varchar(60) as syobuncode,      
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(9) as updated_by
    from transformed
)
select * from final
