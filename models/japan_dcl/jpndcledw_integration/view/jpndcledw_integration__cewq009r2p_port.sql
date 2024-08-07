with TBECPOINTHISTORY as 
(
    select * from SNAPJPDCLITG_INTEGRATION.TBECPOINTHISTORY
)
, TBECORDER as
(
   select * from  SNAPJPDCLITG_INTEGRATION.TBECORDER 
)
, GENOMER_KOKYA_KPI_INBOUND as 
(
    select * from SNAPJPDCLEDW_INTEGRATION.GENOMER_KOKYA_KPI_INBOUND
)
,final as 
(
SELECT CASE 
        WHEN ((his.dipointcode)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
            THEN to_char(COALESCE(odr.dsuriagedt, his.dsren), ('YYYYMMDD'::CHARACTER VARYING)::TEXT)
        ELSE to_char(his.dsren, ('YYYYMMDD'::CHARACTER VARYING)::TEXT)
        END AS "ポイント確定日",
    his.diregistdivcode AS "登録区分",
    COALESCE(((odr.dirouteid)::NUMERIC)::NUMERIC(18, 0), (((99)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(38, 18)) AS "小項目区分",
    his.dipointcode AS "ポイント識別",
    sum(his.dipoint) AS "ポイント数",
    CASE 
        WHEN ((odr.c_dspointitemincludeflg)::TEXT = ('1'::CHARACTER VARYING)::TEXT)
            THEN CASE 
                    WHEN ((his.dipointcode)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
                        THEN odr.c_diexchangepoint
                    ELSE NULL::BIGINT
                    END
        ELSE NULL::BIGINT
        END AS "内訳_交換ｐ"
FROM (
    TBECPOINTHISTORY his LEFT JOIN TBECORDER odr ON ((odr.diorderid = his.diorderid))
    )
WHERE (
        (
            (
                ((his.dielimflg)::TEXT = ((0)::CHARACTER VARYING)::TEXT)
                AND ((his.dipointcode)::TEXT <> ((0)::CHARACTER VARYING)::TEXT)
                )
            AND ((his.divalidflg)::TEXT <> ((0)::CHARACTER VARYING)::TEXT)
            )
        AND (
            NOT (
                his.diecusrid IN (
                    SELECT ib.diusrid
                    FROM GENOMER_KOKYA_KPI_INBOUND ib
                    )
                )
            )
        )
GROUP BY CASE 
        WHEN ((his.dipointcode)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
            THEN to_char(COALESCE(odr.dsuriagedt, his.dsren), ('YYYYMMDD'::CHARACTER VARYING)::TEXT)
        ELSE to_char(his.dsren, ('YYYYMMDD'::CHARACTER VARYING)::TEXT)
        END,
    his.diregistdivcode,
    COALESCE(((odr.dirouteid)::NUMERIC)::NUMERIC(18, 0), (((99)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(38, 18)),
    his.dipointcode,
    CASE 
        WHEN ((odr.c_dspointitemincludeflg)::TEXT = ('1'::CHARACTER VARYING)::TEXT)
            THEN CASE 
                    WHEN ((his.dipointcode)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
                        THEN odr.c_diexchangepoint
                    ELSE NULL::BIGINT
                    END
        ELSE NULL::BIGINT
        END
) 
select * from final 
