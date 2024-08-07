with tm43youbi_nm as
(
    select * from SNAPJPDCLEDW_INTEGRATION.tm43youbi_nm
),final as 
(
SELECT 
tm43youbi_nm.code AS "コード"
, tm43youbi_nm.name AS "名称"
, tm43youbi_nm.cname AS "c名称"
FROM tm43youbi_nm
)
select * from final 