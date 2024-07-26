with tw06_kokyastatus as (
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.tw06_kokyastatus
),
tt05kokyakonyu as (
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.tt05kokyakonyu
),
tm55_teisuchi as (
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.tm55_teisuchi
),
combined_source as (
    select
	    w06.kokyano
        ,t05.hanrocode
        ,t05.syohanrobunname as syobuncode
        ,t05.chuhanrobunname as chubuncode
        ,t05.daihanrobunname as daibuncode
        ,sum(case when to_date(t05.juchdate::STRING,'yyyymmdd')
                    between try_to_date((to_char(current_timestamp(),'YYYYMMDD') - cast(tm55.teisu as numeric))::STRING)
                        and dateadd(day, -1, current_timestamp())
            then 1 else 0
            end) as halfykazu
        ,max(t05.juchdate) as lastjuchdate
        ,max(t05.saleno) as saleno		
	from tw06_kokyastatus w06
	left join tt05kokyakonyu t05 on w06.kokyano=t05.kokyano
	cross join tm55_teisuchi tm55
	where tm55.kubunid = '120'
	group by 
        w06.kokyano,
        t05.hanrocode,
        t05.syohanrobunname,
        t05.chuhanrobunname,
        t05.daihanrobunname
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
    qualify row_number() over(partition by kokyano order by halfykazu desc, lastjuchdate desc, saleno desc) = 1
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
