with 
itg_businesscalender as 
(
    select * from inditg_integration.itg_businesscalender
),
itg_rdssmweeklytarget_output as 
(
    select * from inditg_integration.itg_rdssmweeklytarget_output
),
v_customer_dim as 
(
    select * from indedw_integration.v_customer_dim
),

trans as 
(
    SELECT 
        coalesce(RDSS.Distcode, '-1') as customer_code,
        coalesce(cust.customer_name,'Unknown') as customer_name,
        coalesce(RDSS.SMcode, '-1') as salesman_code,
        coalesce(RDSS.SMName, 'Unknown')  as salesman_name,
        coalesce(RDSS.RMcode, '-1') as route_code,
        coalesce(RDSS.RMName, 'Unknown')  as route_name,
        coalesce(RDSS.TargetYear, '-9999')  as Year,
        CASE WHEN  RDSS.TargetMonth='January'  THEN '1'
            WHEN  RDSS.TargetMonth='February' THEN '2'
            WHEN  RDSS.TargetMonth='March'    THEN '3'
            WHEN  RDSS.TargetMonth='April'    THEN '4'
            WHEN  RDSS.TargetMonth='May'      THEN '5'
            WHEN  RDSS.TargetMonth='June'     THEN '6'
            WHEN  RDSS.TargetMonth='July'     THEN '7'
            WHEN  RDSS.TargetMonth='August'   THEN '8'
            WHEN  RDSS.TargetMonth='September' THEN '9'
            WHEN  RDSS.TargetMonth='October'  THEN '10'
            WHEN  RDSS.TargetMonth='November' THEN '11'
            WHEN  RDSS.TargetMonth='December' THEN '12'
            ELSE 0 end as month,
        cast(substring(gs.weekno,1,1) as integer) as week,
        SUM(CASE WHEN RDSS.targetname='GT' THEN	
                    case when substring(gs.weekno,1,1)='1' THEN RDSS.Week1 
                        when substring(gs.weekno,1,1)='2' THEN RDSS.Week2
                        when substring(gs.weekno,1,1)='3' THEN RDSS.Week3
                        when substring(gs.weekno,1,1)='4' THEN RDSS.Week4
                        when substring(gs.weekno,1,1)='5' THEN RDSS.Week5
                        when substring(gs.weekno,1,1)='0' THEN RDSS.targetvalue
                    end
                ELSE 0 end
                ) as Target_GT,
        SUM(CASE WHEN RDSS.targetname='No.Of Bills' THEN	
                    case when substring(gs.weekno,1,1)='1' THEN RDSS.Week1 
                        when substring(gs.weekno,1,1)='2' THEN RDSS.Week2
                        when substring(gs.weekno,1,1)='3' THEN RDSS.Week3
                        when substring(gs.weekno,1,1)='4' THEN RDSS.Week4
                        when substring(gs.weekno,1,1)='5' THEN RDSS.Week5
                        when substring(gs.weekno,1,1)='0' THEN RDSS.targetvalue
                    end
                    ELSE 0 end) as Target_Bills,
        SUM(CASE WHEN UPPER( RDSS.targetname)='NO.OF PACKS' THEN	
                case when substring(gs.weekno,1,1)='1' THEN RDSS.Week1 
                when substring(gs.weekno,1,1)='2' THEN RDSS.Week2
                when substring(gs.weekno,1,1)='3' THEN RDSS.Week3
                when substring(gs.weekno,1,1)='4' THEN RDSS.Week4
                when substring(gs.weekno,1,1)='5' THEN RDSS.Week5
                when substring(gs.weekno,1,1)='0' THEN RDSS.targetvalue
                end
            ELSE 0 end) as Target_Packs,
        current_timestamp()::timestamp_ntz(9) as CRT_DTTM,
        current_timestamp()::timestamp_ntz(9) as UPDT_DTTM
        FROM (select * from
        (Select 
        row_number() over( partition by distcode, smcode, smname, rmcode, rmname, targetyear, targetmonth, targetname order by targettype desc) rn,
        rd.*
        from  itg_rdssmweeklytarget_output rd
        where TARGETSTATUS='CONFIRMED' AND TARGETYEAR<>'0') rd1
        Where rn=1
        ) RDSS
        LEFT OUTER JOIN v_customer_dim cust on RDSS.Distcode = cust.customer_code
        cross join (select distinct week as weekno from itg_businesscalender where year>=2015 union Select '0 Week' as weekno) gs
        GROUP BY RDSS.Distcode,customer_code,customer_Name,salesman_code,salesman_name,route_code,route_name,Year, Month, cast(substring(gs.weekno,1,1) as integer) 
        order by 1,2,3,4
),
final as 
(
    select 
        customer_code::varchar(50) as customer_code,
        customer_name::varchar(50) as customer_name,
        salesman_code::varchar(50) as salesman_code,
        salesman_name::varchar(50) as salesman_name,
        route_code::varchar(50) as route_code,
        route_name::varchar(50) as route_name,
        year::number(18,0) as year,
        month::number(18,0) as month,
        week::number(18,0) as week,
        target_gt::number(32,8) as target_gt,
        target_bills::number(32,8) as target_bills,
        target_packs::number(32,8) as target_packs,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from trans
)
select * from final