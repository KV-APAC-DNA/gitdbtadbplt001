with edw_sap_bw_dna_material_bomlist as (
    select * from {{ ref('aspedw_integration__edw_sap_bw_dna_material_bomlist') }}
),
final as (
    select 
        bom as "bom",
		plant as "plant",
		matl_num as "matl_num",
		component as "component",
		createdon as "createdon",
		validfrom as "validfrom",
		validto as "validto",
		validfrom_zvlfromc as "validfrom_zvlfromc",
		validto_zvltoi as "validto_zvltoi",
		createdon_id_credat as "createdon_id_credat",
		quantity as "quantity",
		componentscrap as "componentscrap",
		componentunit as "componentunit",
		recordmode as "recordmode",
		basequantity as "basequantity",
		baseunit as "baseunit",
		materialtype as "materialtype",
		zmaterialtype as "zmaterialtype",
		sls_org as "sls_org",
		bomusage as "bomusage",
		alternativebom as "alternativebom",
		bomstatus as "bomstatus",
		componentscrap_zztp as "componentscrap_zztp",
		componentscrap_zzcp as "componentscrap_zzcp",
		fixedquantity as "fixedquantity",
		componentscrap_zausch as "componentscrap_zausch",
		subitemnumber as "subitemnumber",
		matl_desc as "matl_desc",
		subitemquantity as "subitemquantity",
		tolotsize as "tolotsize",
		fromlotsize as "fromlotsize",
		leadtimeoffset as "leadtimeoffset",
		distribution as "distribution",
		operationleadtimeoffset as "operationleadtimeoffset",
		operationleadtimeoffsetunit as "operationleadtimeoffsetunit",
		componentscrap_zzausch as "componentscrap_zzausch",
		operationscrap as "operationscrap",
		operationltoffset as "operationltoffset",
		crt_dttm as "crt_dttm",
		updt_dttm as "updt_dttm"
	from edw_sap_bw_dna_material_bomlist
)
select * from final