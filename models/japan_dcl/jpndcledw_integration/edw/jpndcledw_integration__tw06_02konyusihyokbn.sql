with tw06_01konyujisseki as (
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.tw06_01konyujisseki
),
tm25shihyokubunshikii as (
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.tm25shihyokubunshikii
),
tw06_01konyujisseki_date as 
(
    SELECT
        *,
        ceil((datediff(hour,to_date(lastjuchdate::string,'YYYYMMDD'), current_timestamp())::decimal(20,5))/24) as lastjuchdate_todate,
        ceil((datediff(hour,to_date(lastkonyudate::string,'YYYYMMDD'), current_timestamp())::decimal(20,5))/24) as lastkonyudate_todate
    from 
        tw06_01konyujisseki
),
transformed as (
    SELECT
        f1.kokyano                        as kokyano
        , ifnull(w1.shihyokbncode,'000')  as juchurkbncode
        , ifnull(w2.shihyokbncode,'000')  as konyurkbncode
        , ifnull(w3.shihyokbncode,'000')  as ruifkbncode
        , ifnull(w4.shihyokbncode,'000')  as nenfkbncode
        , ifnull(w5.shihyokbncode,'000')  as ruiikbncode
        , ifnull(w6.shihyokbncode,'000')  as nenikbncode
        , ifnull(w7.shihyokbncode,'000')  as ruimkbncode
        , ifnull(w8.shihyokbncode,'000')  as nenmkbncode1
        , ifnull(w9.shihyokbncode,'000')  as nenmkbncode2
        , ifnull(w10.shihyokbncode,'000') as nenmkbncode3
        , ifnull(w11.shihyokbncode,'000') as nenmkbncode4
        , ifnull(w12.shihyokbncode,'000') as nenmkbncode5
        , '000'                           as tsukigkbncode
    FROM
        tw06_01konyujisseki_date f1
    left join tm25shihyokubunshikii w1
        on f1.lastjuchdate_todate between w1.shikiilwr and w1.shikiiupr and w1.shihyocode = 'JR01'
    left join tm25shihyokubunshikii w2
        on f1.lastkonyudate_todate between w2.shikiilwr and w2.shikiiupr and w2.shihyocode = 'KR01'
    left join tm25shihyokubunshikii w3
       ON  f1.ruikaisu between w3.shikiilwr and w3.shikiiupr and w3.shihyocode = 'RF01'
    left join tm25shihyokubunshikii w4
       on  f1.nenkaisu between w4.shikiilwr and w4.shikiiupr and w4.shihyocode = 'YF01'
    left join tm25shihyokubunshikii w5
        on  f1.ruiindays between w5.shikiilwr and w5.shikiiupr and w5.shihyocode = 'RI01'
    left join tm25shihyokubunshikii w6
        on  f1.nenindays between w6.shikiilwr and w6.shikiiupr and w6.shihyocode = 'YI01'
    left join tm25shihyokubunshikii w7
        on  f1.ruikingaku between w7.shikiilwr and w7.shikiiupr and w7.shihyocode = 'RM01'
    left join tm25shihyokubunshikii w8
        on  f1.nenkingaku between w8.shikiilwr and w8.shikiiupr and w8.shihyocode = 'YM01'
    left join tm25shihyokubunshikii w9
        on  f1.nenkingaku between w9.shikiilwr and w9.shikiiupr and w9.shihyocode = 'YM02'
    left join tm25shihyokubunshikii w10
        on  f1.nenkingaku between w10.shikiilwr and w10.shikiiupr and w10.shihyocode = 'YM03'
    left join tm25shihyokubunshikii w11
        on  f1.nenkingaku between w11.shikiilwr and w11.shikiiupr and w11.shihyocode = 'YM04'
    left join tm25shihyokubunshikii w12
        on  f1.nenkingaku between w12.shikiilwr and w12.shikiiupr and w12.shihyocode = 'YM05'
),
final as (
    select
        kokyano::varchar(15) as kokyano,
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
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(100) as updated_by
    from transformed
)
select * from final
