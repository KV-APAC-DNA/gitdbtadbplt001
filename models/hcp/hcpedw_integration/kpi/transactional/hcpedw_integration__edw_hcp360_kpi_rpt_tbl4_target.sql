{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = " {% if is_incremental() %}
        delete from {{this}} where source_system = 'TARGET';
        {% endif %}"
    )
}}
with 
itg_mds_hcp360_hcp_targets_transpose as 
(
    select * from snapinditg_integration.itg_mds_hcp360_hcp_targets_transpose
),


tempa as 
(
    SELECT
	  'IN' AS COUNTRY
	  ,'TARGET' AS SOURCE_SYSTEM
	  ,CASE WHEN CHANNEL_CODE = 'NonDigital' THEN 'NON_DIGITAL' END AS CHANNEL
	  ,ACTIVITY_TYPE_CODE as ACTIVITY_TYPE    
	  ,YEAR_MONTH as ACTIVITY_DATE
	  ,KPI_CODE as TARGET_KPI
	  ,VALUE as TARGET_VALUE
	  FROM ITG_MDS_HCP360_HCP_TARGETS_TRANSPOSE
	  WHERE KPI_CODE = 'Reach_Index' 
		AND CHANNEL_CODE = 'NonDigital'
		AND ACTIVITY_TYPE_CODE = 'F2F'
		AND VALUE IS NOT NULL
),
tempb as 
(
    SELECT
	  'IN' AS COUNTRY
	  ,'TARGET' AS SOURCE_SYSTEM
	  , CASE WHEN CHANNEL_CODE = 'NonDigital' THEN 'NON_DIGITAL' END AS CHANNEL
	  ,ACTIVITY_TYPE_CODE as ACTIVITY_TYPE    
	  ,YEAR_MONTH as ACTIVITY_DATE
	  ,KPI_CODE as TARGET_KPI
	  ,VALUE as TARGET_VALUE
	  FROM ITG_MDS_HCP360_HCP_TARGETS_TRANSPOSE
	  WHERE KPI_CODE = 'Efficiency_Index' 
		AND CHANNEL_CODE = 'NonDigital'
		AND ACTIVITY_TYPE_CODE = 'F2F'
		AND VALUE IS NOT NULL
),
final as 
(
    select * from tempa
    union all
    select * from tempb
)
select * from final