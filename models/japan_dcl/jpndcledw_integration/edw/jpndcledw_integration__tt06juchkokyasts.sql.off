{{
    config
    (
        materialized="incremental",
        incremental_strategy= "merge",
        unique_key= ["saleno"]
    )
}}

with tw08_juchkokyasts as (
    select * from {{ ref('jpndcledw_integration__tw08_juchkokyasts') }}
),
transformed as (
    select
        trim(w08.saleno) as saleno,
        w08.ruikaisu,
        w08.ruikingaku,
        w08.ruiindays,
        w08.lastjuchdate,
        w08.juchukeikadays,
        w08.lastkonyudate,
        w08.konyukeikadays,
        w08.nenkaisu,
        w08.nenkingaku,
        w08.nenindays,
        w08.nengelryo,
        w08.tsukigelryo,
        w08.juchurkbncode,
        w08.konyurkbncode,
        w08.ruifkbncode,
        w08.nenfkbncode,
        w08.ruiikbncode,
        w08.nenikbncode,
        w08.ruimkbncode,
        w08.nenmkbncode1,
        w08.nenmkbncode2,
        w08.nenmkbncode3,
        w08.nenmkbncode4,
        w08.nenmkbncode5,
        w08.tsukigkbncode,
        w08.segkbncode,
        {% if is_incremental() %}
        case 
            when t06.saleno is not null then t06.insertdate
            else cast(to_char(current_timestamp(),'YYYYMMDD')as integer) 
        end
        {% else %}
            cast(to_char(current_timestamp(),'YYYYMMDD')as integer) 
        {% endif %} as insertdate,
        {% if is_incremental() %}
        case 
            when t06.saleno is not null then t06.inserttime
            else cast(to_char(current_timestamp(),'HH24MISS')as integer)
        end 
        {% else %}
            cast(to_char(current_timestamp(),'HH24MISS')as integer)
        {% endif %} as inserttime,
        '090554' as insertid,
        cast(to_char(current_timestamp(),'YYYYMMDD')as integer) as updatedate,
        cast(to_char(current_timestamp(),'HH24MISS')as integer) as updatetime,
        '090554' as updateid,
        w08.kokyakbncode,
        {% if is_incremental() %}
        case 
            when t06.saleno is not null then 'ETL_Batch'
            else null
        end 
        {% else %}
            null
        {% endif %} as updated_by,
        {% if is_incremental() %}
            t06.bk_saleno
        {% else %}
            null as bk_saleno
        {% endif %}

    from
        tw08_juchkokyasts w08
    {% if is_incremental() %}
    left join {{this}} t06
	    on t06.saleno = w08.saleno
    {% endif %}

),
final as (
    select
        saleno::varchar(18) as saleno,
        ruikaisu::number(18,0) as ruikaisu,
        ruikingaku::number(38,0) as ruikingaku,
        ruiindays::number(18,0) as ruiindays,
        lastjuchdate::number(18,0) as lastjuchdate,
        juchukeikadays::number(18,0) as juchukeikadays,
        lastkonyudate::number(18,0) as lastkonyudate,
        konyukeikadays::number(18,0) as konyukeikadays,
        nenkaisu::number(38,0) as nenkaisu,
        nenkingaku::number(38,0) as nenkingaku,
        nenindays::number(18,0) as nenindays,
        nengelryo::number(18,0) as nengelryo,
        tsukigelryo::number(6,1) as tsukigelryo,
        juchurkbncode::varchar(4) as juchurkbncode,
        konyurkbncode::varchar(4) as konyurkbncode,
        ruifkbncode::varchar(4) as ruifkbncode,
        nenfkbncode::varchar(4) as nenfkbncode,
        ruiikbncode::varchar(4) as ruiikbncode,
        nenikbncode::varchar(4) as nenikbncode,
        ruimkbncode::varchar(4) as ruimkbncode,
        nenmkbncode1::varchar(4) as nenmkbncode1,
        nenmkbncode2::varchar(4) as nenmkbncode2,
        nenmkbncode3::varchar(4) as nenmkbncode3,
        nenmkbncode4::varchar(4) as nenmkbncode4,
        nenmkbncode5::varchar(4) as nenmkbncode5,
        tsukigkbncode::varchar(4) as tsukigkbncode,
        segkbncode::varchar(3) as segkbncode,
        insertdate::number(18,0) as insertdate,
        inserttime::number(18,0) as inserttime,
        insertid::varchar(9) as insertid,
        updatedate::number(18,0) as updatedate,
        updatetime::number(18,0) as updatetime,
        updateid::varchar(9) as updateid,
        kokyakbncode::varchar(3) as kokyakbncode,
        bk_saleno::varchar(18) as bk_saleno,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        updated_by::varchar(100) as updated_by
    from transformed
)
select * from final
