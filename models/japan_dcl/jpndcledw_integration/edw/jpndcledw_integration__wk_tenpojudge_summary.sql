WITH KESAI_H_DATA_MART_MV
AS
(
    SELECT * FROM snapjpdcledw_integration.KESAI_H_DATA_MART_MV
),
WK_POINT_HANROJUDGE AS
(
    SELECT * FROM snapjpdcledw_integration.WK_POINT_HANROJUDGE
),
final as
(
    SELECT JUCH.KOKYANO::VARCHAR(68) as KOKYANO
      ,(CASE WHEN TORIKEIKBN = '01' THEN '通販' --'01 通販'
            WHEN TORIKEIKBN = '03' THEN '店舗' --'03 直営店買取'
            WHEN TORIKEIKBN = '04' THEN '店舗' --'04 直営店'
            WHEN TORIKEIKBN = '05' THEN '店舗' --'05 消化取引'
       END)::VARCHAR(6) AS TORIKEIKBN
      ,MAX(JUCHDATE)::NUMBER(18,0) AS MAX
      ,COUNT(1)::NUMBER(38,0) as CNT
    FROM KESAI_H_DATA_MART_MV JUCH
    WHERE JUCH.JUCHDATE BETWEEN
    TO_NUMBER(TO_CHAR(ADD_MONTHS(CONVERT_TIMEZONE('UTC','Asia/Tokyo','2024-07-01'::date),-36),'YYYYMMDD'),'9999999999') AND TO_NUMBER(TO_CHAR(dateadd(day,-181,(CONVERT_TIMEZONE('UTC','Asia/Tokyo','2024-07-01'::date))),'YYYYMMDD'),'9999999999')
    
    -- redshift
    -- TO_NUMBER(TO_CHAR(ADD_MONTHS(CONVERT_TIMEZONE('UTC','Asia/Tokyo',SYSDATE()),-36),'YYYYMMDD'),'9999999999') AND TO_NUMBER(TO_CHAR(dateadd(day,-181,(CONVERT_TIMEZONE('UTC','Asia/Tokyo',SYSDATE()))),'YYYYMMDD'),'9999999999') --181D-3Y対応
    AND   EXISTS(SELECT 'X'
                FROM  KESAI_H_DATA_MART_MV
                WHERE  KOKYANO = JUCH.KOKYANO
                    AND  TORIKEIKBN IN ('03','04','05'))
    AND   EXISTS(SELECT 'X'
                FROM  WK_POINT_HANROJUDGE
                WHERE  LPAD(DIUSRID,10,0) = JUCH.KOKYANO)
    AND   NOT(JUCH.MAKER = 2 AND JUCH.KAKOKBN = 0 AND JUCH.SOGOKEI = 0)
    GROUP BY  JUCH.KOKYANO
            ,CASE WHEN TORIKEIKBN = '01' THEN '通販'--'01 通販'
                WHEN TORIKEIKBN = '03' THEN '店舗'--'03 直営店買取'
                WHEN TORIKEIKBN = '04' THEN '店舗'--'04 直営店'
                WHEN TORIKEIKBN = '05' THEN '店舗'--'05 消化取引'
            END
)
select * from final