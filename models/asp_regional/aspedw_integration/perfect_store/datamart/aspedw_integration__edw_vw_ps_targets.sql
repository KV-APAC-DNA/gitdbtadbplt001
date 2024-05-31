-- with itg_mds_cn_ps_targets as (
--     select * from --Not sure where this model needs to be created
--                     snapntaitg_integration.itg_mds_cn_ps_targets
-- ),
-- itg_mds_pacific_ps_targets as (
--     select * from -- ref('pcfitg_integration__itg_mds_pacific_ps_targets') 
--                     snappcfitg_integration.itg_mds_pacific_ps_targets
-- ),
-- itg_mds_jp_ps_targets as (
--     select * from -- ref('jpnitg_integration__itg_mds_jp_ps_targets') 
--             snapntaitg_integration.itg_mds_jp_ps_targets
-- ),
-- itg_mds_hk_ps_targets as (
--     select * from -- ref('ntaitg_integration__itg_mds_hk_ps_targets') 
--                 snapntaitg_integration.itg_mds_hk_ps_targets
-- ),
-- itg_mds_tw_ps_targets as (
--     select * from -- ref('ntaitg_integration__itg_mds_tw_ps_targets')
--             snapntaitg_integration.itg_mds_tw_ps_targets
-- ),
-- itg_mds_kr_ps_targets as (
--     select * from -- ref('ntaitg_integration__itg_mds_kr_ps_targets')
--             snapntaitg_integration.itg_mds_kr_ps_targets
-- ),
-- itg_mds_in_ps_targets as (
--     select * from -- ref('inditg_integration__itg_mds_in_ps_targets')
--             snapinditg_integration.itg_mds_in_ps_targets
-- ),
with 
itg_id_ps_targets as (
    select * from -- ref('idnitg_integration__itg_id_ps_targets')
            {{ref('idnitg_integration__itg_id_ps_targets')}}
)
-- ,itg_mds_sg_ps_targets as (
--     select * from -- ref('sgpitg_integration__itg_mds_sg_ps_targets') 
-- ),
-- itg_mds_th_ps_targets as (
--     select * from -- ref('thaitg_integration__itg_mds_th_ps_targets')
-- ),
-- itg_mds_vn_ps_targets as (
--     select * from -- ref('vnmitg_integration__itg_mds_vn_ps_targets') 
-- ),
-- itg_mds_ph_ps_targets as (
--     select * from -- ref('phlitg_integration__itg_mds_ph_ps_targets')
-- ),
-- itg_mds_my_ps_targets as (
--     select * from -- ref('itg_mds_my_ps_targets') Ref Model name currectly here
--                 snapntaitg_integration.itg_mds_my_ps_targets
-- )



-- SELECT 'China' AS market
-- 	,itg_mds_cn_ps_targets.kpi
-- 	,itg_mds_cn_ps_targets.channel
-- 	,itg_mds_cn_ps_targets.retail_env AS retail_environment
-- 	,itg_mds_cn_ps_targets.attribute_1
-- 	,itg_mds_cn_ps_targets.attribute_2
-- 	,itg_mds_cn_ps_targets."target" AS value
-- FROM itg_mds_cn_ps_targets

-- UNION ALL

-- SELECT itg_mds_pacific_ps_targets.market
-- 	,itg_mds_pacific_ps_targets.kpi
-- 	,itg_mds_pacific_ps_targets.channel
-- 	,itg_mds_pacific_ps_targets.retail_env AS retail_environment
-- 	,itg_mds_pacific_ps_targets.attribute_1
-- 	,itg_mds_pacific_ps_targets.attribute_2
-- 	,itg_mds_pacific_ps_targets."target" AS value
-- FROM itg_mds_pacific_ps_targets

-- UNION ALL

-- SELECT 'Japan' AS market
-- 	,itg_mds_jp_ps_targets.kpi
-- 	,itg_mds_jp_ps_targets.channel
-- 	,itg_mds_jp_ps_targets.retail_env AS retail_environment
-- 	,itg_mds_jp_ps_targets.attribute_1
-- 	,itg_mds_jp_ps_targets.attribute_2
-- 	,itg_mds_jp_ps_targets."target" AS value
-- FROM itg_mds_jp_ps_targets

-- UNION ALL

-- SELECT 'Hong Kong' AS market
-- 	,itg_mds_hk_ps_targets.kpi
-- 	,itg_mds_hk_ps_targets.channel
-- 	,itg_mds_hk_ps_targets.retail_env AS retail_environment
-- 	,itg_mds_hk_ps_targets.attribute_1
-- 	,itg_mds_hk_ps_targets.attribute_2
-- 	,itg_mds_hk_ps_targets."target" AS value
-- FROM itg_mds_hk_ps_targets

-- UNION ALL

-- SELECT 'Taiwan' AS market
-- 	,itg_mds_tw_ps_targets.kpi
-- 	,itg_mds_tw_ps_targets.channel
-- 	,itg_mds_tw_ps_targets.retail_env AS retail_environment
-- 	,itg_mds_tw_ps_targets.attribute_1
-- 	,itg_mds_tw_ps_targets.attribute_2
-- 	,itg_mds_tw_ps_targets."target" AS value
-- FROM itg_mds_tw_ps_targets

-- UNION ALL

-- SELECT 'Korea' AS market
-- 	,itg_mds_kr_ps_targets.kpi
-- 	,itg_mds_kr_ps_targets.channel
-- 	,itg_mds_kr_ps_targets.retail_env AS retail_environment
-- 	,itg_mds_kr_ps_targets.attribute_1
-- 	,itg_mds_kr_ps_targets.attribute_2
-- 	,itg_mds_kr_ps_targets."target" AS value
-- FROM itg_mds_kr_ps_targets

-- UNION ALL

-- SELECT 'India' AS market
-- 	,itg_mds_in_ps_targets.kpi
-- 	,itg_mds_in_ps_targets.channel
-- 	,itg_mds_in_ps_targets.retail_env AS retail_environment
-- 	,itg_mds_in_ps_targets.attribute_1
-- 	,itg_mds_in_ps_targets.attribute_2
-- 	,itg_mds_in_ps_targets."target" AS value
-- FROM itg_mds_in_ps_targets

-- UNION ALL

SELECT 'Indonesia' AS market
	,itg_id_ps_targets.kpi
	,itg_id_ps_targets.channel
	,itg_id_ps_targets.retail_env AS retail_environment
	,itg_id_ps_targets.attribute_1
	,itg_id_ps_targets.attribute_2
	,itg_id_ps_targets.target AS value
FROM itg_id_ps_targets

-- UNION ALL

-- SELECT 'Singapore' AS market
-- 	,itg_mds_sg_ps_targets.kpi
-- 	,itg_mds_sg_ps_targets.channel
-- 	,itg_mds_sg_ps_targets.retail_env AS retail_environment
-- 	,itg_mds_sg_ps_targets.attribute_1
-- 	,itg_mds_sg_ps_targets.attribute_2
-- 	,itg_mds_sg_ps_targets."target" AS value
-- FROM itg_mds_sg_ps_targets

-- UNION ALL

-- SELECT 'Thailand' AS market
-- 	,itg_mds_th_ps_targets.kpi
-- 	,itg_mds_th_ps_targets.channel
-- 	,itg_mds_th_ps_targets.retail_env AS retail_environment
-- 	,itg_mds_th_ps_targets.attribute_1
-- 	,itg_mds_th_ps_targets.attribute_2
-- 	,itg_mds_th_ps_targets."target" AS value
-- FROM itg_mds_th_ps_targets

-- UNION ALL

-- SELECT 'Vietnam' AS market
-- 	,itg_mds_vn_ps_targets.kpi
-- 	,itg_mds_vn_ps_targets.channel
-- 	,itg_mds_vn_ps_targets.retail_env AS retail_environment
-- 	,itg_mds_vn_ps_targets.attribute_1
-- 	,itg_mds_vn_ps_targets.attribute_2
-- 	,itg_mds_vn_ps_targets."target" AS value
-- FROM itg_mds_vn_ps_targets

-- UNION ALL

-- SELECT 'Philippines' AS market
-- 	,itg_mds_ph_ps_targets.kpi
-- 	,itg_mds_ph_ps_targets.channel
-- 	,itg_mds_ph_ps_targets.retail_env AS retail_environment
-- 	,itg_mds_ph_ps_targets.attribute_1
-- 	,itg_mds_ph_ps_targets.attribute_2
-- 	,itg_mds_ph_ps_targets."target" AS value
-- FROM itg_mds_ph_ps_targets

-- UNION ALL

-- SELECT 'Malaysia' AS market
-- 	,itg_mds_my_ps_targets.kpi
-- 	,itg_mds_my_ps_targets.channel
-- 	,itg_mds_my_ps_targets.retail_env AS retail_environment
-- 	,itg_mds_my_ps_targets.attribute_1
-- 	,itg_mds_my_ps_targets.attribute_2
-- 	,itg_mds_my_ps_targets."target" AS value
-- FROM itg_mds_my_ps_targets