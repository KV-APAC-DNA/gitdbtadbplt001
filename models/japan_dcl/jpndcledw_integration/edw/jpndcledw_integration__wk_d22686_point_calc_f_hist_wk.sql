WITH TBECORDER
AS (
    SELECT *
    FROM JPDCLITG_INTEGRATION.TBECORDER
    ),
C_TBECKESAI
AS (
    SELECT *
    FROM JPDCLITG_INTEGRATION.C_TBECKESAI
    ),
KR_COMM_POINT_PARA
AS (
    SELECT *
    FROM JPDCLEDW_INTEGRATION.KR_COMM_POINT_PARA
    ),
final
AS (
    SELECT ORD.DIORDERID AS ORD_DIORDERID,
        ORD.DIECUSRID AS ORD_DIECUSRID,
        TO_CHAR(ORD.DSORDERDT, 'YYYYMMDD') AS ORD_DSORDERDT,
        ORD.DITOTALPRC AS ORD_DITOTALPRC,
        ORD.DIHAISOPRC AS ORD_DIHAISOPRC,
        ORD.DIUSEPOINT AS ORD_DIUSEPOINT,
        ORD.DIPOINT AS ORD_DIPOINT,
        ORD.DIHORYU AS ORD_DIHORYU,
        ORD.DICANCEL AS ORD_DICANCEL,
        ORD.DISHUKKASTS AS ORD_DISHUKKASTS,
        TO_CHAR(ORD.DSURIAGEDT, 'YYYYMMDD') AS ORD_DSURIAGEDT,
        ORD.DIORDERTAX AS ORD_DIORDERTAX,
        ORD.C_DIDISCOUNTPRC AS ORD_C_DIDISCOUNTPRC,
        ORD.C_DIDISCOUNTALL AS ORD_C_DIDISCOUNTALL,
        ORD.DISEIKYUPRC AS ORD_DISEIKYUPRC,
        TO_CHAR(ORD.C_DSORDERREFERDATE, 'YYYYMMDD') AS ORD_C_DSORDERREFERDATE,
        ORD.C_DSORDERKBN AS ORD_C_DSORDERKBN,
        ORD.DIELIMFLG AS ORD_DIELIMFLG,
        KES.C_DIKESAIID AS KES_C_DIKESAIID,
        KES.DISHUKKASTS AS KES_DISHUKKASTS,
        TO_CHAR(KES.DSURIAGEDT, 'YYYYMMDD') AS KES_DSURIAGEDT,
        TO_CHAR(KES.C_DSSHORTTODOKEDATE, 'YYYYMMDD') AS KES_C_DSSHORTTODOKEDATE,
        TO_CHAR(KES.C_DSTODOKEDATE, 'YYYYMMDD') AS KES_C_DSTODOKEDATE,
        TO_CHAR(KES.C_DSSHUKKADATE, 'YYYYMMDD') AS KES_C_DSSHUKKADATE,
        KES.DITOTALPRC AS KES_DITOTALPRC,
        KES.C_DIDISCOUNTPRC AS KES_C_DIDISCOUNTPRC,
        KES.C_DIDISCOUNTALL AS KES_C_DIDISCOUNTALL,
        KES.DIORDERTAX AS KES_DIORDERTAX,
        KES.DIHAISOPRC AS KES_DIHAISOPRC,
        KES.C_DICOLLECTPRC AS KES_C_DICOLLECTPRC,
        KES.DISEIKYUPRC AS KES_DISEIKYUPRC,
        KES.DIUSEPOINT AS KES_DIUSEPOINT,
        KES.DIELIMFLG AS KES_DIELIMFLG
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
