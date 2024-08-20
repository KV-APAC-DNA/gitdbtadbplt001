{{
    config
    (
        pre_hook = 
                    "DELETE FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w1') }} 						
                    where not exists(select 'X' FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_wz') }} T1 where T1.ITEMCODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_w1') }}.ITEMCODE)						
                        or not exists(select 'X' FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_wz') }} T2 where T2.ITEMCODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_w1') }}.KOSECODE)						
                        or {{ ref('jpndcledw_integration__item_jizen_bunkai_w1') }}.ITEMCODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_w1') }}.KOSECODE;"
    )
}}

with ITEM_JIZEN_BUNKAI_W1 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w1') }}
),


trns as
(	
    SELECT					
	 2           AS KAISOU					
	,T1.ITEMCODE AS ITEMCODE					
	,T1.KOSECODE AS KOSECODE					
	,T1.SURYO    AS SURYO					
	 FROM ITEM_JIZEN_BUNKAI_W1 T1					
	 UNION ALL					
	 SELECT					
	 3           AS KAISOU					
	,T1.ITEMCODE AS ITEMCODE					
	,T2.KOSECODE AS KOSECODE					
	,ROUND(T1.SURYO * T2.SURYO , 4) AS SURYO					
	 FROM ITEM_JIZEN_BUNKAI_W1 T1					
	 JOIN ITEM_JIZEN_BUNKAI_W1 T2 ON T1.KOSECODE = T2.ITEMCODE					
	 UNION ALL					
	 SELECT					
	 4           AS KAISOU					
	,T1.ITEMCODE AS ITEMCODE					
	,T3.KOSECODE AS KOSECODE					
	,ROUND(T1.SURYO * T2.SURYO * T3.SURYO , 4) AS SURYO					
	 FROM ITEM_JIZEN_BUNKAI_W1 T1					
	 JOIN ITEM_JIZEN_BUNKAI_W1 T2 ON T1.KOSECODE = T2.ITEMCODE					
	 JOIN ITEM_JIZEN_BUNKAI_W1 T3 ON T2.KOSECODE = T3.ITEMCODE					
	 UNION ALL					
	 SELECT					
	 5           AS KAISOU					
	,T1.ITEMCODE AS ITEMCODE					
	,T4.KOSECODE AS KOSECODE					
	,ROUND(T1.SURYO * T2.SURYO * T3.SURYO * T4.SURYO , 4) AS SURYO					
	 FROM ITEM_JIZEN_BUNKAI_W1 T1					
	 JOIN ITEM_JIZEN_BUNKAI_W1 T2 ON T1.KOSECODE = T2.ITEMCODE					
	 JOIN ITEM_JIZEN_BUNKAI_W1 T3 ON T2.KOSECODE = T3.ITEMCODE					
	 JOIN ITEM_JIZEN_BUNKAI_W1 T4 ON T3.KOSECODE = T4.ITEMCODE					
	 UNION ALL					
	 SELECT					
	 6           AS KAISOU					
	,T1.ITEMCODE AS ITEMCODE					
	,T5.KOSECODE AS KOSECODE					
	,ROUND(T1.SURYO * T2.SURYO * T3.SURYO * T4.SURYO * T5.SURYO , 4) AS SURYO					
	 FROM ITEM_JIZEN_BUNKAI_W1 T1					
	 JOIN ITEM_JIZEN_BUNKAI_W1 T2 ON T1.KOSECODE = T2.ITEMCODE					
	 JOIN ITEM_JIZEN_BUNKAI_W1 T3 ON T2.KOSECODE = T3.ITEMCODE					
	 JOIN ITEM_JIZEN_BUNKAI_W1 T4 ON T3.KOSECODE = T4.ITEMCODE					
	 JOIN ITEM_JIZEN_BUNKAI_W1 T5 ON T4.KOSECODE = T5.ITEMCODE
),

final as
(
    select
        KAISOU::NUMBER(18,0) as KAISOU,
        ITEMCODE::VARCHAR(40) as ITEMCODE,
        KOSECODE::VARCHAR(40) as KOSECODE,
        SURYO::NUMBER(35,4) as SURYO
    from trns
)

select * from final