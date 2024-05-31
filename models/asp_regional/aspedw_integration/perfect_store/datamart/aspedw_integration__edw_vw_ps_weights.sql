with itg_id_ps_weights as(
	select * from {{ ref('idnitg_integration__itg_id_ps_weights') }}
),
transformed as(
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
								FROM itg_id_ps_weights
)
select * from transformed