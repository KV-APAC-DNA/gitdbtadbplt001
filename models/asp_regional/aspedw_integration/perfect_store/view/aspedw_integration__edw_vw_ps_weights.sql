/* Please add sources cte later, I dont have time as I have to build indonesia model. 
Commenting Other markets out
cc: Anjali */


-- SELECT 'Vietnam' AS market
-- 	,(
-- 		CASE 
-- 			WHEN (upper((itg_mds_vn_ps_weights.kpi)::TEXT) = 'OOS Compliance'::TEXT)
-- 				THEN 'OSA COMPLIANCE'::TEXT
-- 			WHEN (upper((itg_mds_vn_ps_weights.kpi)::TEXT) = 'Planogram'::TEXT)
-- 				THEN 'Planogram compliance'::TEXT
-- 			WHEN (upper((itg_mds_vn_ps_weights.kpi)::TEXT) = 'promo'::TEXT)
-- 				THEN 'Promo Compliance'::TEXT
-- 			WHEN (upper((itg_mds_vn_ps_weights.kpi)::TEXT) = 'Share of Assortment'::TEXT)
-- 				THEN 'soa compliance'::TEXT
-- 			WHEN (upper((itg_mds_vn_ps_weights.kpi)::TEXT) = 'Share of Shelf'::TEXT)
-- 				THEN 'sos compliance'::TEXT
-- 			ELSE upper((itg_mds_vn_ps_weights.kpi)::TEXT)
-- 			END
-- 		)::CHARACTER VARYING AS kpi
-- 	,itg_mds_vn_ps_weights.channel
-- 	,itg_mds_vn_ps_weights.retail_env AS retail_environment
-- 	,itg_mds_vn_ps_weights.weight
-- FROM os_itg.itg_mds_vn_ps_weights

-- UNION ALL

-- SELECT 'Thailand' AS market
-- 	,(
-- 		CASE 
-- 			WHEN (upper((itg_mds_th_ps_weights.kpi)::TEXT) = 'OOS Compliance'::TEXT)
-- 				THEN 'OSA COMPLIANCE'::TEXT
-- 			WHEN (upper((itg_mds_th_ps_weights.kpi)::TEXT) = 'Planogram'::TEXT)
-- 				THEN 'Planogram compliance'::TEXT
-- 			WHEN (upper((itg_mds_th_ps_weights.kpi)::TEXT) = 'promo'::TEXT)
-- 				THEN 'Promo Compliance'::TEXT
-- 			WHEN (upper((itg_mds_th_ps_weights.kpi)::TEXT) = 'Share of Assortment'::TEXT)
-- 				THEN 'soa compliance'::TEXT
-- 			WHEN (upper((itg_mds_th_ps_weights.kpi)::TEXT) = 'Share of Shelf'::TEXT)
-- 				THEN 'sos compliance'::TEXT
-- 			ELSE upper((itg_mds_th_ps_weights.kpi)::TEXT)
-- 			END
-- 		)::CHARACTER VARYING AS kpi
-- 	,itg_mds_th_ps_weights.channel
-- 	,itg_mds_th_ps_weights.retail_env AS retail_environment
-- 	,itg_mds_th_ps_weights.weight
-- FROM os_itg.itg_mds_th_ps_weights

-- UNION ALL

-- SELECT 'Philippines' AS market
-- 	,(
-- 		CASE 
-- 			WHEN (upper((itg_mds_ph_ps_weights.kpi)::TEXT) = 'OOS Compliance'::TEXT)
-- 				THEN 'OSA COMPLIANCE'::TEXT
-- 			WHEN (upper((itg_mds_ph_ps_weights.kpi)::TEXT) = 'Planogram'::TEXT)
-- 				THEN 'Planogram compliance'::TEXT
-- 			WHEN (upper((itg_mds_ph_ps_weights.kpi)::TEXT) = 'promo'::TEXT)
-- 				THEN 'Promo Compliance'::TEXT
-- 			WHEN (upper((itg_mds_ph_ps_weights.kpi)::TEXT) = 'Share of Assortment'::TEXT)
-- 				THEN 'soa compliance'::TEXT
-- 			WHEN (upper((itg_mds_ph_ps_weights.kpi)::TEXT) = 'Share of Shelf'::TEXT)
-- 				THEN 'sos compliance'::TEXT
-- 			ELSE upper((itg_mds_ph_ps_weights.kpi)::TEXT)
-- 			END
-- 		)::CHARACTER VARYING AS kpi
-- 	,itg_mds_ph_ps_weights.channel
-- 	,itg_mds_ph_ps_weights.retail_env AS retail_environment
-- 	,itg_mds_ph_ps_weights.weight
-- FROM os_itg.itg_mds_ph_ps_weights

-- UNION ALL

-- SELECT 'Singapore' AS market
-- 	,(
-- 		CASE 
-- 			WHEN (upper((itg_mds_sg_ps_weights.kpi)::TEXT) = 'OOS Compliance'::TEXT)
-- 				THEN 'OSA COMPLIANCE'::TEXT
-- 			WHEN (upper((itg_mds_sg_ps_weights.kpi)::TEXT) = 'Planogram'::TEXT)
-- 				THEN 'Planogram compliance'::TEXT
-- 			WHEN (upper((itg_mds_sg_ps_weights.kpi)::TEXT) = 'promo'::TEXT)
-- 				THEN 'Promo Compliance'::TEXT
-- 			WHEN (upper((itg_mds_sg_ps_weights.kpi)::TEXT) = 'Share of Assortment'::TEXT)
-- 				THEN 'soa compliance'::TEXT
-- 			WHEN (upper((itg_mds_sg_ps_weights.kpi)::TEXT) = 'Share of Shelf'::TEXT)
-- 				THEN 'sos compliance'::TEXT
-- 			ELSE upper((itg_mds_sg_ps_weights.kpi)::TEXT)
-- 			END
-- 		)::CHARACTER VARYING AS kpi
-- 	,itg_mds_sg_ps_weights.channel
-- 	,itg_mds_sg_ps_weights.retail_env AS retail_environment
-- 	,itg_mds_sg_ps_weights.weight
-- FROM os_itg.itg_mds_sg_ps_weights

-- UNION ALL

-- SELECT 'Malaysia' AS market
-- 	,(
-- 		CASE 
-- 			WHEN (upper((itg_mds_my_ps_weights.kpi)::TEXT) = 'OOS Compliance'::TEXT)
-- 				THEN 'OSA COMPLIANCE'::TEXT
-- 			WHEN (upper((itg_mds_my_ps_weights.kpi)::TEXT) = 'Planogram'::TEXT)
-- 				THEN 'Planogram compliance'::TEXT
-- 			WHEN (upper((itg_mds_my_ps_weights.kpi)::TEXT) = 'promo'::TEXT)
-- 				THEN 'Promo Compliance'::TEXT
-- 			WHEN (upper((itg_mds_my_ps_weights.kpi)::TEXT) = 'Share of Assortment'::TEXT)
-- 				THEN 'soa compliance'::TEXT
-- 			WHEN (upper((itg_mds_my_ps_weights.kpi)::TEXT) = 'Share of Shelf'::TEXT)
-- 				THEN 'sos compliance'::TEXT
-- 			ELSE upper((itg_mds_my_ps_weights.kpi)::TEXT)
-- 			END
-- 		)::CHARACTER VARYING AS kpi
-- 	,itg_mds_my_ps_weights.channel
-- 	,itg_mds_my_ps_weights.retail_env AS retail_environment
-- 	,itg_mds_my_ps_weights.weight
-- FROM os_itg.itg_mds_my_ps_weights

-- UNION ALL

SELECT 'Indonesia' AS market
	,(
		CASE 
			WHEN (upper((itg_id_ps_weights.kpi)::TEXT) = 'OOS Compliance'::TEXT)
				THEN 'OSA COMPLIANCE'::TEXT
			WHEN (upper((itg_id_ps_weights.kpi)::TEXT) = 'Planogram'::TEXT)
				THEN 'Planogram compliance'::TEXT
			WHEN (upper((itg_id_ps_weights.kpi)::TEXT) = 'promo'::TEXT)
				THEN 'Promo Compliance'::TEXT
			WHEN (upper((itg_id_ps_weights.kpi)::TEXT) = 'Share of Assortment'::TEXT)
				THEN 'soa compliance'::TEXT
			WHEN (upper((itg_id_ps_weights.kpi)::TEXT) = 'Share of Shelf'::TEXT)
				THEN 'sos compliance'::TEXT
			ELSE upper((itg_id_ps_weights.kpi)::TEXT)
			END
		)::CHARACTER VARYING AS kpi
	,itg_id_ps_weights.channel
	,itg_id_ps_weights.retail_env AS retail_environment
	,itg_id_ps_weights.weight
FROM -- ref('idnitg_integration__itg_id_ps_weights') 
    snapidnitg_integration.itg_id_ps_weights

-- UNION ALL

-- SELECT 'Taiwan' AS market
-- 	,(
-- 		CASE 
-- 			WHEN (upper((itg_mds_tw_ps_weights.kpi)::TEXT) = 'OOS Compliance'::TEXT)
-- 				THEN 'OSA COMPLIANCE'::TEXT
-- 			WHEN (upper((itg_mds_tw_ps_weights.kpi)::TEXT) = 'Planogram'::TEXT)
-- 				THEN 'Planogram compliance'::TEXT
-- 			WHEN (upper((itg_mds_tw_ps_weights.kpi)::TEXT) = 'promo'::TEXT)
-- 				THEN 'Promo Compliance'::TEXT
-- 			WHEN (upper((itg_mds_tw_ps_weights.kpi)::TEXT) = 'Share of Assortment'::TEXT)
-- 				THEN 'soa compliance'::TEXT
-- 			WHEN (upper((itg_mds_tw_ps_weights.kpi)::TEXT) = 'Share of Shelf'::TEXT)
-- 				THEN 'sos compliance'::TEXT
-- 			ELSE upper((itg_mds_tw_ps_weights.kpi)::TEXT)
-- 			END
-- 		)::CHARACTER VARYING AS kpi
-- 	,itg_mds_tw_ps_weights.channel
-- 	,itg_mds_tw_ps_weights.retail_env AS retail_environment
-- 	,itg_mds_tw_ps_weights.weight
-- FROM na_itg.itg_mds_tw_ps_weights

-- UNION ALL

-- SELECT 'Korea' AS market
-- 	,(
-- 		CASE 
-- 			WHEN (upper((itg_mds_kr_ps_weights.kpi)::TEXT) = 'OOS Compliance'::TEXT)
-- 				THEN 'OSA COMPLIANCE'::TEXT
-- 			WHEN (upper((itg_mds_kr_ps_weights.kpi)::TEXT) = 'Planogram'::TEXT)
-- 				THEN 'Planogram compliance'::TEXT
-- 			WHEN (upper((itg_mds_kr_ps_weights.kpi)::TEXT) = 'promo'::TEXT)
-- 				THEN 'Promo Compliance'::TEXT
-- 			WHEN (upper((itg_mds_kr_ps_weights.kpi)::TEXT) = 'Share of Assortment'::TEXT)
-- 				THEN 'soa compliance'::TEXT
-- 			WHEN (upper((itg_mds_kr_ps_weights.kpi)::TEXT) = 'Share of Shelf'::TEXT)
-- 				THEN 'sos compliance'::TEXT
-- 			ELSE upper((itg_mds_kr_ps_weights.kpi)::TEXT)
-- 			END
-- 		)::CHARACTER VARYING AS kpi
-- 	,itg_mds_kr_ps_weights.channel
-- 	,itg_mds_kr_ps_weights.retail_env AS retail_environment
-- 	,itg_mds_kr_ps_weights.weight
-- FROM na_itg.itg_mds_kr_ps_weights

-- UNION ALL

-- SELECT 'Hong Kong' AS market
-- 	,(
-- 		CASE 
-- 			WHEN (upper((itg_mds_hk_ps_weights.kpi)::TEXT) = 'OOS Compliance'::TEXT)
-- 				THEN 'OSA COMPLIANCE'::TEXT
-- 			WHEN (upper((itg_mds_hk_ps_weights.kpi)::TEXT) = 'Planogram'::TEXT)
-- 				THEN 'Planogram compliance'::TEXT
-- 			WHEN (upper((itg_mds_hk_ps_weights.kpi)::TEXT) = 'promo'::TEXT)
-- 				THEN 'Promo Compliance'::TEXT
-- 			WHEN (upper((itg_mds_hk_ps_weights.kpi)::TEXT) = 'Share of Assortment'::TEXT)
-- 				THEN 'soa compliance'::TEXT
-- 			WHEN (upper((itg_mds_hk_ps_weights.kpi)::TEXT) = 'Share of Shelf'::TEXT)
-- 				THEN 'sos compliance'::TEXT
-- 			ELSE upper((itg_mds_hk_ps_weights.kpi)::TEXT)
-- 			END
-- 		)::CHARACTER VARYING AS kpi
-- 	,itg_mds_hk_ps_weights.channel
-- 	,itg_mds_hk_ps_weights.retail_env AS retail_environment
-- 	,itg_mds_hk_ps_weights.weight
-- FROM na_itg.itg_mds_hk_ps_weights

-- UNION ALL

-- SELECT 'Japan' AS market
-- 	,(
-- 		CASE 
-- 			WHEN (upper((itg_mds_jp_ps_weights.kpi)::TEXT) = 'OOS Compliance'::TEXT)
-- 				THEN 'OSA COMPLIANCE'::TEXT
-- 			WHEN (upper((itg_mds_jp_ps_weights.kpi)::TEXT) = 'Planogram'::TEXT)
-- 				THEN 'Planogram compliance'::TEXT
-- 			WHEN (upper((itg_mds_jp_ps_weights.kpi)::TEXT) = 'promo'::TEXT)
-- 				THEN 'Promo Compliance'::TEXT
-- 			WHEN (upper((itg_mds_jp_ps_weights.kpi)::TEXT) = 'Share of Assortment'::TEXT)
-- 				THEN 'soa compliance'::TEXT
-- 			WHEN (upper((itg_mds_jp_ps_weights.kpi)::TEXT) = 'Share of Shelf'::TEXT)
-- 				THEN 'sos compliance'::TEXT
-- 			ELSE upper((itg_mds_jp_ps_weights.kpi)::TEXT)
-- 			END
-- 		)::CHARACTER VARYING AS kpi
-- 	,itg_mds_jp_ps_weights.channel
-- 	,itg_mds_jp_ps_weights.retail_env AS retail_environment
-- 	,itg_mds_jp_ps_weights.weight
-- FROM jp_itg.itg_mds_jp_ps_weights

-- UNION ALL

-- SELECT 'India' AS market
-- 	,(
-- 		CASE 
-- 			WHEN (upper((itg_mds_in_ps_weights.kpi)::TEXT) = 'OOS Compliance'::TEXT)
-- 				THEN 'OSA COMPLIANCE'::TEXT
-- 			WHEN (upper((itg_mds_in_ps_weights.kpi)::TEXT) = 'Planogram'::TEXT)
-- 				THEN 'Planogram compliance'::TEXT
-- 			WHEN (upper((itg_mds_in_ps_weights.kpi)::TEXT) = 'promo'::TEXT)
-- 				THEN 'Promo Compliance'::TEXT
-- 			WHEN (upper((itg_mds_in_ps_weights.kpi)::TEXT) = 'Share of Assortment'::TEXT)
-- 				THEN 'soa compliance'::TEXT
-- 			WHEN (upper((itg_mds_in_ps_weights.kpi)::TEXT) = 'Share of Shelf'::TEXT)
-- 				THEN 'sos compliance'::TEXT
-- 			ELSE upper((itg_mds_in_ps_weights.kpi)::TEXT)
-- 			END
-- 		)::CHARACTER VARYING AS kpi
-- 	,itg_mds_in_ps_weights.channel
-- 	,itg_mds_in_ps_weights.retail_env AS retail_environment
-- 	,itg_mds_in_ps_weights.weight
-- FROM in_itg.itg_mds_in_ps_weights

-- UNION ALL

-- SELECT 'China' AS market
-- 	,(
-- 		CASE 
-- 			WHEN (upper((itg_cn_ps_weights.kpi)::TEXT) = 'OOS Compliance'::TEXT)
-- 				THEN 'OSA COMPLIANCE'::TEXT
-- 			WHEN (upper((itg_cn_ps_weights.kpi)::TEXT) = 'Planogram'::TEXT)
-- 				THEN 'Planogram compliance'::TEXT
-- 			WHEN (upper((itg_cn_ps_weights.kpi)::TEXT) = 'promo'::TEXT)
-- 				THEN 'Promo Compliance'::TEXT
-- 			WHEN (upper((itg_cn_ps_weights.kpi)::TEXT) = 'Share of Assortment'::TEXT)
-- 				THEN 'soa compliance'::TEXT
-- 			WHEN (upper((itg_cn_ps_weights.kpi)::TEXT) = 'Share of Shelf'::TEXT)
-- 				THEN 'sos compliance'::TEXT
-- 			ELSE upper((itg_cn_ps_weights.kpi)::TEXT)
-- 			END
-- 		)::CHARACTER VARYING AS kpi
-- 	,itg_cn_ps_weights.channel
-- 	,itg_cn_ps_weights.re AS retail_environment
-- 	,itg_cn_ps_weights.weight
-- FROM cn_itg.itg_cn_ps_weights

-- UNION ALL

-- SELECT itg_mds_pacific_ps_weights.market
-- 	,(
-- 		CASE 
-- 			WHEN (upper((itg_mds_pacific_ps_weights.kpi)::TEXT) = 'OOS Compliance'::TEXT)
-- 				THEN 'OSA COMPLIANCE'::TEXT
-- 			WHEN (upper((itg_mds_pacific_ps_weights.kpi)::TEXT) = 'Planogram'::TEXT)
-- 				THEN 'Planogram compliance'::TEXT
-- 			WHEN (upper((itg_mds_pacific_ps_weights.kpi)::TEXT) = 'promo'::TEXT)
-- 				THEN 'Promo Compliance'::TEXT
-- 			WHEN (upper((itg_mds_pacific_ps_weights.kpi)::TEXT) = 'Share of Assortment'::TEXT)
-- 				THEN 'soa compliance'::TEXT
-- 			WHEN (upper((itg_mds_pacific_ps_weights.kpi)::TEXT) = 'Share of Shelf'::TEXT)
-- 				THEN 'sos compliance'::TEXT
-- 			ELSE upper((itg_mds_pacific_ps_weights.kpi)::TEXT)
-- 			END
-- 		)::CHARACTER VARYING AS kpi
-- 	,itg_mds_pacific_ps_weights.channel
-- 	,itg_mds_pacific_ps_weights.retail_env AS retail_environment
-- 	,itg_mds_pacific_ps_weights.weight
-- FROM au_itg.itg_mds_pacific_ps_weights
