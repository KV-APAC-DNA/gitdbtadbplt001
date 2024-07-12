WITH tbusrpram
AS (
    SELECT *
    FROM dev_dna_core.snapjpdclitg_integration.tbusrpram
    ),
tbecorder
AS (
    SELECT *
    FROM dev_dna_core.snapjpdclitg_integration.tbecorder
    ),
cust_info
AS (
    SELECT '1' AS diusridcontrol,
        dstel AS tel,
        diusrid
    FROM tbusrpram
    
    UNION ALL
    
    SELECT '2' AS diusridcontrol,
        dsdat2 AS tel,
        diusrid
    FROM tbusrpram
    
    UNION ALL
    
    SELECT '3' AS diusridcontrol,
        dsdat3 AS tel,
        diusrid
    FROM tbusrpram
    ),
cust_order
AS (
    SELECT diusrid
    FROM (
        SELECT diusrid AS diusrid
        FROM tbusrpram
        WHERE to_date(dsprep, 'yyyy-mm-dd hh24:mi:ss') >= to_date(dateadd(month, - 12, current_timestamp()))
            AND disecessionflg = '0'
            AND dielimflg = '0'
        
        UNION
        
        SELECT od.diecusrid AS diusrid
        FROM tbecorder od
        INNER JOIN tbusrpram up ON up.diusrid = od.diecusrid
        WHERE od.dsorderdt >= to_date(dateadd(month, - 36, current_timestamp()))
            AND od.dicancel = '0'
            AND od.dielimflg = '0'
            AND up.disecessionflg = '0'
            AND up.dielimflg = '0'
        )
    GROUP BY diusrid
    ),
transformed
AS (
    SELECT cust_info.diusrid AS diusrid,
        diusridcontrol,
        tel
    FROM cust_info
    INNER JOIN cust_order ON cust_info.diusrid = cust_order.diusrid
    WHERE tel IS NOT NULL
    ),

final as
(
    select 
        diusrid::VARCHAR(16) AS diusrid,
        diusridcontrol::VARCHAR(16) AS diusridcontrol,
        tel::VARCHAR(16) AS tel
    from transformed
)
SELECT *
FROM final