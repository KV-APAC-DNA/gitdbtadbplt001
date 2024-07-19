with tw06_kokyastatus as (
    select * from dev_dna_core.snapjpdcledw_integration.tw06_kokyastatus
),
tt05kokyakonyu as (
    select * from dev_dna_core.snapjpdcledw_integration.tt05kokyakonyu
),

tm55_teisuchi as (
    select * from dev_dna_core.snapjpdcledw_integration.tm55_teisuchi
),
combined_source as (
    select
	    w06.kokyano
        ,t05.tenpocode
        ,sum(case when to_date(t05.juchdate::string,'YYYYMMDD')
                    between dateadd(day, 0 - cast(tm55.teisu as numeric), current_timestamp())
                        and dateadd(day, -1, current_timestamp())
            then 1 else 0
            end) as halfykazu	
        ,max(t05.juchdate) as lastjuchdate
	from tw06_kokyastatus w06
	left join tt05kokyakonyu t05 on w06.kokyano=t05.kokyano
	cross join tm55_teisuchi tm55
	where tm55.kubunid = '120'
    and t05.torikeikbn in ('03','04','05')
	group by 
        w06.kokyano,
        t05.tenpocode
),
zero_source as (
    select
        w06.kokyano
        ,sum(case when t05.torikeikbn in ('03','04','05') then 1 else 0 end) as tenpokazu  
    from tw06_kokyastatus w06
    left join tt05kokyakonyu t05 on w06.kokyano=t05.kokyano
    group by w06.kokyano   
),	
transformed as (
    select
        kokyano
        ,ifnull(tenpocode,'00000') as tenpocode  
    from 
        combined_source
    qualify row_number() over(partition by kokyano order by halfykazu desc, lastjuchdate desc, tenpocode desc) = 1

    union 

    select 
        kokyano
        ,'00000'  as tenpocode  
    from 
        zero_source
    where
        tenpokazu = 0
),
final as (
    select
        kokyano::varchar(15) as kokyano,
        tenpocode::varchar(7) as tenpocode,     
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(100) as updated_by
    from transformed
)
select * from final
