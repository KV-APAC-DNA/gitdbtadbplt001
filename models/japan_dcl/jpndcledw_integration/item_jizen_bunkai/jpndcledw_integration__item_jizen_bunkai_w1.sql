with bom_sap_v as
(
    select * from {{ ref('jpndcledw_integration__bom_sap_v') }}
),

item_bom_ikou_kizuna as
(
    select * from {{ source('jpdcledw_integration', 'item_bom_ikou_kizuna') }}
),

trns as
(	
    SELECT					
		BOM_sap.OYA_HIN_CD AS ITEMCODE ,				
		BOM_sap.KOS_HIN_CD AS KOSECODE ,				
		NVL(BOM_sap.KOS_QUT,0)::NUMERIC(28,4) AS SURYO				
	FROM					
		bom_sap_v BOM_sap				
	where					
		    BOM_sap.kos_hin_cd not like '009%'				
		AND BOM_sap.kos_hin_cd not like '0082%'				
		AND BOM_sap.kos_hin_cd not like '019%'				
		AND BOM_sap.kos_hin_cd not like '0182%'				
	union					
	SELECT					
		BOM_ikou.OYA_HIN_CD AS ITEMCODE ,				
		BOM_ikou.KOS_HIN_CD AS KOSECODE ,				
		NVL(BOM_ikou.KOS_QUT,0)::NUMERIC(28,4) AS SURYO				
	FROM					
		item_bom_ikou_kizuna BOM_ikou				
	where					
		not exists (				
		select				
			1			
		from				
			bom_sap_v BOM_sap			
		where				
			BOM_sap.OYA_HIN_CD = BOM_ikou.OYA_HIN_CD 
        )
),

final as
(
    select
        ITEMCODE::VARCHAR(40) as ITEMCODE,
        KOSECODE::VARCHAR(40) as KOSECODE,
        SURYO::NUMBER(28,4) as SURYO
    from trns
)

select * from final