{{
    config
    (
        pre_hook = 
                    "DELETE FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w2') }}						
                    WHERE EXISTS(SELECT 'X' FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w2') }} T2 WHERE {{ ref('jpndcledw_integration__item_jizen_bunkai_w2') }}.KOSECODE = T2.ITEMCODE);",
        post_hook = "
                    DELETE FROM {{this}}					
                    WHERE EXISTS(						
                                    SELECT	'X'	
                                    FROM	{{ source('jpdcledw_integration', 'tm14shkos_mainte_work') }} T2	
                                    WHERE	{{this}}.ITEMCODE = T2.ITEMCODE	
                                    AND		T2.KOSECODE NOT LIKE '009%'
                                    AND		T2.KOSECODE NOT LIKE '0082%'
                                    AND		T2.KOSECODE NOT LIKE '019%'
                                    AND		T2.KOSECODE NOT LIKE '0182%'     
                                    );
                    insert into {{this}} (ITEMCODE, KOSECODE, SURYO)						
                    SELECT						
                        TM14.ITEMCODE                          AS ITEMCODE						
                        ,TM14.KOSECODE                          AS KOSECODE						
                        ,TM14.SURYO                             AS SURYO						
                    FROM {{ source('jpdcledw_integration', 'tm14shkos_mainte_work') }} TM14						
                                            
                    WHERE						
                        TM14.KOSECODE NOT LIKE '009%'						
                    AND  TM14.KOSECODE NOT LIKE '0082%'						
                    AND  TM14.KOSECODE NOT LIKE '019%'						
                    AND  TM14.KOSECODE NOT LIKE '0182%';
                    "
    )
}}

with ITEM_JIZEN_BUNKAI_W2 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w2') }}
),


trns as
(	
    SELECT 						
	 T.ITEMCODE   AS ITEMCODE					
	,T.KOSECODE   AS KOSECODE					
	,SUM(T.SURYO) AS SURYO					
  FROM ITEM_JIZEN_BUNKAI_W2 T						
  GROUP BY T.ITEMCODE, T.KOSECODE
),

final as
(
    select
        ITEMCODE::VARCHAR(40) as ITEMCODE,
        KOSECODE::VARCHAR(40) as KOSECODE,
        SURYO::NUMBER(35,4) as SURYO
    from trns
)

select * from final