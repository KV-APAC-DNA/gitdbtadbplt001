with c_tbecranksumamount as (
    select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.c_tbecranksumamount
),
tbusrpram as (
    select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.tbusrpram
),
kr_comm_point_para as (
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.kr_comm_point_para
),
filterd_tbecranksumamount as (
    select
        DIECUSRID as USRID,
        C_DSAGGREGATEYM as RANKDT,
        C_DSRANKTOTALPRCBYMONTH as PRC
    from 
        c_tbecranksumamount
    where
        DIELIMFLG='0' AND  
        substring(C_DSAGGREGATEYM,1,6) between (select target_year +'01' from kr_comm_point_para) and (select substring(term_end,1,6) from kr_comm_point_para )
),
filterd_tbusrpram as (
    SELECT
        DIUSRID
    FROM
        tbusrpram
    WHERE   
        DIELIMFLG='0' AND  
        DISECESSIONFLG='0' AND  
        DSDAT93='通常ユーザ'
),
final as(
    SELECT
        A.USRID,
        A.RANKDT,
        SUM(A.PRC) as PRICE
    FROM
        filterd_tbecranksumamount A

    WHERE EXISTS 
    (SELECT
        'X'
    FROM
        TBUSRPRAM B
    WHERE   
        A.USRID=B.DIUSRID)
    GROUP BY
        A.USRID,
        A.RANKDT
    HAVING 
        SUM(A.PRC)>=1
)
select * from final
