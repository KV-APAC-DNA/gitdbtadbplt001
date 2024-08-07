WITH TBECORDER
AS (
    SELECT *
    FROM {{ref('jpndclitg_integration__tbecorder')}}
    ),
C_TBECKESAI
AS (
    SELECT *
    FROM {{ref('jpndclitg_integration__c_tbeckesai')}}
    ),
KR_COMM_POINT_PARA
AS (
    SELECT *
    FROM SNAPJPDCLEDW_INTEGRATION.KR_COMM_POINT_PARA 
    ),
final
AS (
    SELECT ORD.DIORDERID::NUMBER(38,0) AS ORD_DIORDERID,
        ORD.DIECUSRID::NUMBER(38,0) AS ORD_DIECUSRID,
        TO_CHAR(ORD.DSORDERDT, 'YYYYMMDD')::VARCHAR(16) AS ORD_DSORDERDT,
        ORD.DITOTALPRC::NUMBER(38,0) AS ORD_DITOTALPRC,
        ORD.DIHAISOPRC::NUMBER(38,0) AS ORD_DIHAISOPRC,
        ORD.DIUSEPOINT::NUMBER(38,0) AS ORD_DIUSEPOINT,
        ORD.DIPOINT::NUMBER(38,0) AS ORD_DIPOINT,
        ORD.DIHORYU::VARCHAR(1) AS ORD_DIHORYU,
        ORD.DICANCEL::VARCHAR(1) AS ORD_DICANCEL,
        ORD.DISHUKKASTS::VARCHAR(6) AS ORD_DISHUKKASTS,
        TO_CHAR(ORD.DSURIAGEDT, 'YYYYMMDD')::VARCHAR(16) AS ORD_DSURIAGEDT,
        ORD.DIORDERTAX::NUMBER(38,0) AS ORD_DIORDERTAX,
        ORD.C_DIDISCOUNTPRC::NUMBER(38,0) AS ORD_C_DIDISCOUNTPRC,
        ORD.C_DIDISCOUNTALL::NUMBER(38,0) AS ORD_C_DIDISCOUNTALL,
        ORD.DISEIKYUPRC::NUMBER(38,0) AS ORD_DISEIKYUPRC,
        TO_CHAR(ORD.C_DSORDERREFERDATE, 'YYYYMMDD')::VARCHAR(16) AS ORD_C_DSORDERREFERDATE,
        ORD.C_DSORDERKBN::VARCHAR(1) AS ORD_C_DSORDERKBN,
        ORD.DIELIMFLG::VARCHAR(1) AS ORD_DIELIMFLG,
        KES.C_DIKESAIID::NUMBER(38,0) AS KES_C_DIKESAIID,
        KES.DISHUKKASTS::VARCHAR(6) AS KES_DISHUKKASTS,
        TO_CHAR(KES.DSURIAGEDT, 'YYYYMMDD')::VARCHAR(16) AS KES_DSURIAGEDT,
        TO_CHAR(KES.C_DSSHORTTODOKEDATE, 'YYYYMMDD')::VARCHAR(16) AS KES_C_DSSHORTTODOKEDATE,
        TO_CHAR(KES.C_DSTODOKEDATE, 'YYYYMMDD')::VARCHAR(16) AS KES_C_DSTODOKEDATE,
        TO_CHAR(KES.C_DSSHUKKADATE, 'YYYYMMDD')::VARCHAR(16) AS KES_C_DSSHUKKADATE,
        KES.DITOTALPRC::NUMBER(38,0) AS KES_DITOTALPRC,
        KES.C_DIDISCOUNTPRC::NUMBER(38,0) AS KES_C_DIDISCOUNTPRC,
        KES.C_DIDISCOUNTALL::NUMBER(38,0) AS KES_C_DIDISCOUNTALL,
        KES.DIORDERTAX::NUMBER(38,0) AS KES_DIORDERTAX,
        KES.DIHAISOPRC::NUMBER(38,0) AS KES_DIHAISOPRC,
        KES.C_DICOLLECTPRC::NUMBER(38,0) AS KES_C_DICOLLECTPRC,
        KES.DISEIKYUPRC::NUMBER(38,0) AS KES_DISEIKYUPRC,
        KES.DIUSEPOINT::NUMBER(38,0) AS KES_DIUSEPOINT,
        KES.DIELIMFLG::VARCHAR(1) AS KES_DIELIMFLG
    FROM TBECORDER ORD
    INNER JOIN C_TBECKESAI KES ON KES.DIORDERID = ORD.DIORDERID
    WHERE
        --Modification for correction 20210114 Start
        -- (TO_CHAR(ORD.C_DSORDERREFERDATE,'YYYYMMDD')BETWEEN (SUBSTRING((select Term_End from jp_dcl_edw.KR_COMM_POINT_PARA),1,6)||'01')  AND 
        (
            TO_CHAR(ORD.C_DSORDERREFERDATE, 'YYYYMMDD') BETWEEN (
                        CASE 
                            WHEN (
                                    SELECT substring(term_end, 7, 2)
                                    FROM KR_COMM_POINT_PARA
                                    ) < 15
                                THEN (
                                        SELECT to_char(add_months(to_date(substring(term_end, 1, 6), 'YYYYMM'), - 1), 'YYYYMM') || '01'
                                        FROM KR_COMM_POINT_PARA
                                        )
                            ELSE (
                                    SUBSTRING((
                                            SELECT Term_End
                                            FROM KR_COMM_POINT_PARA
                                            ), 1, 6) || '01'
                                    )
                            END
                        )
                AND 
                    --Modification for correction 20210114 End
                    (
                        SELECT Term_End
                        FROM KR_COMM_POINT_PARA
                        )
            )
        AND ORD.DIROUTEID IN (1, 2, 3, 4, 5, 7, 8, 9, 10, 11)
        AND ORD.DICANCEL = 0
        AND ORD.DIELIMFLG = 0
        AND KES.DIELIMFLG = 0
        AND KES.DISEIKYUPRC > 0
    ORDER BY 1
    )
SELECT *
FROM final
