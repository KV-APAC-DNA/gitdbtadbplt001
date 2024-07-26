with cim01kokya
as
(
    select * from jpdcledw_integration.cim01kokya
),
final as
(
    SELECT ck.kokyano::VARCHAR(60) AS KOKYANO,
        ck.zipcode::VARCHAR(30) AS ZIPCODE,
        ck.todofukenname::VARCHAR(120) AS TODOFUKENNAME,
        ck.seibetukbn::VARCHAR(760) AS SEIBETUKBN,
        ck.birthday::NUMBER(38,0) AS BIRTHDAY,
        substring (((ck.birthday)::CHARACTER VARYING)::TEXT,5) ::VARCHAR(16777216) AS BIRTHDAY_MD,
        ck.kokyakbn::VARCHAR(12000) AS KOKYAKBN,
        ck.dmtenpoflg::VARCHAR(12000) AS DMTENPOFLG,
        ck.dmtsuhanflg::VARCHAR(12000) AS DMTSUHANFLG,
        ck.teltenpoflg::VARCHAR(12000) AS TELTENPOFLG,
        ck.teltsuhanflg::VARCHAR(12000) AS TELTSUHANFLG,
        ck.firstmediacode::VARCHAR(20) AS FIRSTMEDIACODE,
        ck.taikai_flg::VARCHAR(760) AS TAIKAI_FLG,
        ck.webno::VARCHAR(48) AS WEBNO,
        ck.insertdate::NUMBER(38,0) AS REGISTER_DT
FROM cim01kokya ck
WHERE ((ck.testusrflg)::TEXT = ('通常ユーザ'::CHARACTER VARYING)::TEXT)
)
select * from final