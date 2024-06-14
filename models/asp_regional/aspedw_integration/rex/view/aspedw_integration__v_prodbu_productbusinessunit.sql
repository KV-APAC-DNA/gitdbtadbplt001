with prodbu_productbusinessunit as (
select * from DEV_DNA_LOAD.SNAPASPSDL_RAW.PRODBU_PRODUCTBUSINESSUNIT
),
final as (SELECT rank() OVER (
		PARTITION BY t1.region
		,t1.productid
		,CASE 
			WHEN (
					((t1.hier1)::TEXT = ('Hong Kong'::CHARACTER VARYING)::TEXT)
					OR (
						(t1.hier1 IS NULL)
						AND ('Hong Kong' IS NULL)
						)
					)
				THEN 'Hongkong'::CHARACTER VARYING
			ELSE t1.hier1
			END ORDER BY t1.azuredatetime DESC
		) AS rank
	,t1.region
	,t1.fetcheddatetime
	,t1.fetchedsequence
	,t1.azurefile
	,t1.azuredatetime
	,t1.productbusinessunitid
	,t1.remotekey
	,t1.productid
	,t1.productremotekey
	,t1.producttext
	,t1.producttype
	,t1.businessunitid
	,t1.businessunitremotekey
	,t1.businessunittext
	,t1.businessunittype
	,t1.hier1id
	,t1.hier1
	,t1.hier2id
	,t1.hier2
	,t1.hier3id
	,t1.hier3
	,t1.hier4id
	,t1.hier4
	,t1.hier5id
	,t1.hier5
	,t1.hier6id
	,t1.hier6
	,t1.hier7id
	,t1.hier7
	,t1.hier8id
	,t1.hier8
	,t1.deliveryunit
	,t1.islisted
	,t1.isorderable
	,t1.isreturnable
	,t1.maximumorderquantity
	,t1.salesunitofmeasure
	,t1.cdl_datetime
	,t1.cdl_source_file
	,t1.load_key
FROM prodbu_productbusinessunit t1)
select * from final 
