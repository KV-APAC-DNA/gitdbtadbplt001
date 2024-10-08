WITH WK_BIRTHDAY_HANRO
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__wk_birthday_hanro') }}
    ),
TBUSRPRAM
AS (
    SELECT *
    FROM {{ ref('jpndclitg_integration__tbusrpram') }}
    ),
FINAL
AS (
    SELECT BD.DIUSRID::NUMBER(10, 0) AS DIUSRID,
        BD.HANRO::VARCHAR(40) AS HANRO,
        BD.SHUKADATE::VARCHAR(32) AS SHUKADATE,
        USR.DSDAT60::VARCHAR(6000) AS DSDAT60,
        USR.DSDAT61::VARCHAR(6000) AS DSDAT61
    FROM WK_BIRTHDAY_HANRO BD
    INNER JOIN TBUSRPRAM USR ON BD.DIUSRID = USR.DIUSRID
    WHERE BD.HANRO = '通販'
        AND --THIS TWO CONDITIONS ARE TEMP
        TO_NUMBER(BD.SHUKADATE, '9999999999') <= TO_NUMBER(TO_CHAR(DATEADD(DAY, - 181, convert_timezone('Asia/Tokyo', current_timestamp())), 'YYYYMMDD'), '9999999999')
        AND TO_NUMBER(BD.SHUKADATE, '9999999999') >= TO_NUMBER(TO_CHAR(DATEADD(DAY, - 1 - 365 * 3, convert_timezone('Asia/Tokyo', current_timestamp())), 'YYYYMMDD'), '9999999999')
        --THIS NEEDS TO BE KEPT
        --TO_NUMBER(BD.SHUKADATE, '9999999999') <= TO_NUMBER(TO_CHAR(DATEADD(DAY,-181,convert_timezone('Asia/Tokyo', current_timestamp())), 'YYYYMMDD'), '9999999999') AND TO_NUMBER(BD.SHUKADATE, '9999999999') >= TO_NUMBER(TO_CHAR(DATEADD(DAY,-1-365*3,convert_timezone('Asia/Tokyo', current_timestamp())), 'YYYYMMDD'), '9999999999')
        AND USR.DSDAT60 = '拒否'
        AND USR.DSDAT61 = '拒否'
    )
SELECT *
FROM FINAL
