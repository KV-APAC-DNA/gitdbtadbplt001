with itg_id_ps_weights as(
	select * from {{ ref('idnitg_integration__itg_id_ps_weights') }}
 ),
itg_mds_pacific_ps_weights as(
	select * from {{ ref('pcfitg_integration__itg_mds_pacific_ps_weights') }} 
),
itg_mds_vn_ps_weights as(
	select * from {{ ref('vnmitg_integration__itg_mds_vn_ps_weights') }} 
),
itg_mds_th_ps_weights as(
	select * from {{ ref('thaitg_integration__itg_mds_th_ps_weights') }} 
),
itg_mds_ph_ps_weights as(
	select * from {{ ref('phlitg_integration__itg_mds_ph_ps_weights') }} 
),
itg_mds_sg_ps_weights as(
	select * from {{ ref('sgpitg_integration__itg_mds_sg_ps_weights') }} 
),
itg_mds_tw_ps_weights as(
	select * from {{ ref('ntaitg_integration__itg_mds_tw_ps_weights') }} 
),
itg_mds_kr_ps_weights as(
	select * from {{ ref('ntaitg_integration__itg_mds_kr_ps_weights') }} 
),
itg_mds_hk_ps_weights as(
	select * from {{ ref('ntaitg_integration__itg_mds_hk_ps_weights') }} 
),
itg_mds_in_ps_weights  as(
	select * from inditg_integration.itg_mds_in_ps_weights
),
itg_mds_jp_ps_weights  as(
	select * from jpnitg_integration.itg_mds_jp_ps_weights
),
itg_mds_my_ps_weights as(
	select * from {{ ref('mysitg_integration__itg_mds_my_ps_weights') }} 
),
indonesia as 
(
    SELECT 
        'Indonesia' AS market,
        (
            CASE
                WHEN (
                    upper((itg_id_ps_weights.kpi)::TEXT) = 'OOS Compliance'::TEXT
                ) THEN 'OSA COMPLIANCE'::TEXT
                WHEN (
                    upper((itg_id_ps_weights.kpi)::TEXT) = 'Planogram'::TEXT
                ) THEN 'Planogram compliance'::TEXT
                WHEN (
                    upper((itg_id_ps_weights.kpi)::TEXT) = 'promo'::TEXT
                ) THEN 'Promo Compliance'::TEXT
                WHEN (
                    upper((itg_id_ps_weights.kpi)::TEXT) = 'Share of Assortment'::TEXT
                ) THEN 'soa compliance'::TEXT
                WHEN (
                    upper((itg_id_ps_weights.kpi)::TEXT) = 'Share of Shelf'::TEXT
                ) THEN 'sos compliance'::TEXT
                ELSE upper((itg_id_ps_weights.kpi)::TEXT)
            END
        )::CHARACTER VARYING AS kpi,
        itg_id_ps_weights.channel,
        itg_id_ps_weights.retail_env AS retail_environment,
        itg_id_ps_weights.weight
    FROM itg_id_ps_weights
),
vietnam as
(
    SELECT 
        'Vietnam' AS market,
        (
            CASE
                WHEN (
                    upper((itg_mds_vn_ps_weights.kpi)::TEXT) = 'OOS Compliance'::TEXT
                ) THEN 'OSA COMPLIANCE'::TEXT
                WHEN (
                    upper((itg_mds_vn_ps_weights.kpi)::TEXT) = 'Planogram'::TEXT
                ) THEN 'Planogram compliance'::TEXT
                WHEN (
                    upper((itg_mds_vn_ps_weights.kpi)::TEXT) = 'promo'::TEXT
                ) THEN 'Promo Compliance'::TEXT
                WHEN (
                    upper((itg_mds_vn_ps_weights.kpi)::TEXT) = 'Share of Assortment'::TEXT
                ) THEN 'soa compliance'::TEXT
                WHEN (
                    upper((itg_mds_vn_ps_weights.kpi)::TEXT) = 'Share of Shelf'::TEXT
                ) THEN 'sos compliance'::TEXT
                ELSE upper((itg_mds_vn_ps_weights.kpi)::TEXT)
            END
        )::CHARACTER VARYING AS kpi,
        itg_mds_vn_ps_weights.channel,
        itg_mds_vn_ps_weights.retail_env AS retail_environment,
        itg_mds_vn_ps_weights.weight
    FROM itg_mds_vn_ps_weights
),
thailand as
(
    SELECT 
        'Thailand' AS market,
        (
            CASE
                WHEN (
                    upper((itg_mds_th_ps_weights.kpi)::TEXT) = 'OOS Compliance'::TEXT
                ) THEN 'OSA COMPLIANCE'::TEXT
                WHEN (
                    upper((itg_mds_th_ps_weights.kpi)::TEXT) = 'Planogram'::TEXT
                ) THEN 'Planogram compliance'::TEXT
                WHEN (
                    upper((itg_mds_th_ps_weights.kpi)::TEXT) = 'promo'::TEXT
                ) THEN 'Promo Compliance'::TEXT
                WHEN (
                    upper((itg_mds_th_ps_weights.kpi)::TEXT) = 'Share of Assortment'::TEXT
                ) THEN 'soa compliance'::TEXT
                WHEN (
                    upper((itg_mds_th_ps_weights.kpi)::TEXT) = 'Share of Shelf'::TEXT
                ) THEN 'sos compliance'::TEXT
                ELSE upper((itg_mds_th_ps_weights.kpi)::TEXT)
            END
        )::CHARACTER VARYING AS kpi,
        itg_mds_th_ps_weights.channel,
        itg_mds_th_ps_weights.retail_env AS retail_environment,
        itg_mds_th_ps_weights.weight
    FROM itg_mds_th_ps_weights
),
philippines as 
(
    SELECT 
        'Philippines' AS market,
        (
            CASE
                WHEN (
                    upper((itg_mds_ph_ps_weights.kpi)::TEXT) = 'OOS Compliance'::TEXT
                ) THEN 'OSA COMPLIANCE'::TEXT
                WHEN (
                    upper((itg_mds_ph_ps_weights.kpi)::TEXT) = 'Planogram'::TEXT
                ) THEN 'Planogram compliance'::TEXT
                WHEN (
                    upper((itg_mds_ph_ps_weights.kpi)::TEXT) = 'promo'::TEXT
                ) THEN 'Promo Compliance'::TEXT
                WHEN (
                    upper((itg_mds_ph_ps_weights.kpi)::TEXT) = 'Share of Assortment'::TEXT
                ) THEN 'soa compliance'::TEXT
                WHEN (
                    upper((itg_mds_ph_ps_weights.kpi)::TEXT) = 'Share of Shelf'::TEXT
                ) THEN 'sos compliance'::TEXT
                ELSE upper((itg_mds_ph_ps_weights.kpi)::TEXT)
            END
        )::CHARACTER VARYING AS kpi,
        itg_mds_ph_ps_weights.channel,
        itg_mds_ph_ps_weights.retail_env AS retail_environment,
        itg_mds_ph_ps_weights.weight
    FROM itg_mds_ph_ps_weights
),
singapore as
(
    SELECT 
        'Singapore' AS market,
        (
            CASE
                WHEN (
                    upper((itg_mds_sg_ps_weights.kpi)::TEXT) = 'OOS Compliance'::TEXT
                ) THEN 'OSA COMPLIANCE'::TEXT
                WHEN (
                    upper((itg_mds_sg_ps_weights.kpi)::TEXT) = 'Planogram'::TEXT
                ) THEN 'Planogram compliance'::TEXT
                WHEN (
                    upper((itg_mds_sg_ps_weights.kpi)::TEXT) = 'promo'::TEXT
                ) THEN 'Promo Compliance'::TEXT
                WHEN (
                    upper((itg_mds_sg_ps_weights.kpi)::TEXT) = 'Share of Assortment'::TEXT
                ) THEN 'soa compliance'::TEXT
                WHEN (
                    upper((itg_mds_sg_ps_weights.kpi)::TEXT) = 'Share of Shelf'::TEXT
                ) THEN 'sos compliance'::TEXT
                ELSE upper((itg_mds_sg_ps_weights.kpi)::TEXT)
            END
        )::CHARACTER VARYING AS kpi,
        itg_mds_sg_ps_weights.channel,
        itg_mds_sg_ps_weights.retail_env AS retail_environment,
        itg_mds_sg_ps_weights.weight
    FROM itg_mds_sg_ps_weights
),
malaysia as
(
    SELECT 
        'Malaysia' AS market,
        (
            CASE
                WHEN (
                    upper((itg_mds_my_ps_weights.kpi)::TEXT) = 'OOS Compliance'::TEXT
                ) THEN 'OSA COMPLIANCE'::TEXT
                WHEN (
                    upper((itg_mds_my_ps_weights.kpi)::TEXT) = 'Planogram'::TEXT
                ) THEN 'Planogram compliance'::TEXT
                WHEN (
                    upper((itg_mds_my_ps_weights.kpi)::TEXT) = 'promo'::TEXT
                ) THEN 'Promo Compliance'::TEXT
                WHEN (
                    upper((itg_mds_my_ps_weights.kpi)::TEXT) = 'Share of Assortment'::TEXT
                ) THEN 'soa compliance'::TEXT
                WHEN (
                    upper((itg_mds_my_ps_weights.kpi)::TEXT) = 'Share of Shelf'::TEXT
                ) THEN 'sos compliance'::TEXT
                ELSE upper((itg_mds_my_ps_weights.kpi)::TEXT)
            END
        )::CHARACTER VARYING AS kpi,
        itg_mds_my_ps_weights.channel,
        itg_mds_my_ps_weights.retail_env AS retail_environment,
        itg_mds_my_ps_weights.weight
    FROM itg_mds_my_ps_weights
),
taiwan as
(
    SELECT 
        'Taiwan' AS market,
        (
            CASE
                WHEN (
                    upper((itg_mds_tw_ps_weights.kpi)::TEXT) = 'OOS Compliance'::TEXT
                ) THEN 'OSA COMPLIANCE'::TEXT
                WHEN (
                    upper((itg_mds_tw_ps_weights.kpi)::TEXT) = 'Planogram'::TEXT
                ) THEN 'Planogram compliance'::TEXT
                WHEN (
                    upper((itg_mds_tw_ps_weights.kpi)::TEXT) = 'promo'::TEXT
                ) THEN 'Promo Compliance'::TEXT
                WHEN (
                    upper((itg_mds_tw_ps_weights.kpi)::TEXT) = 'Share of Assortment'::TEXT
                ) THEN 'soa compliance'::TEXT
                WHEN (
                    upper((itg_mds_tw_ps_weights.kpi)::TEXT) = 'Share of Shelf'::TEXT
                ) THEN 'sos compliance'::TEXT
                ELSE upper((itg_mds_tw_ps_weights.kpi)::TEXT)
            END
        )::CHARACTER VARYING AS kpi,
        itg_mds_tw_ps_weights.channel,
        itg_mds_tw_ps_weights.retail_env AS retail_environment,
        itg_mds_tw_ps_weights.weight
    FROM itg_mds_tw_ps_weights
),
korea as
(
    SELECT 
        'Korea' AS market,
        (
            CASE
                WHEN (
                    upper((itg_mds_kr_ps_weights.kpi)::TEXT) = 'OOS Compliance'::TEXT
                ) THEN 'OSA COMPLIANCE'::TEXT
                WHEN (
                    upper((itg_mds_kr_ps_weights.kpi)::TEXT) = 'Planogram'::TEXT
                ) THEN 'Planogram compliance'::TEXT
                WHEN (
                    upper((itg_mds_kr_ps_weights.kpi)::TEXT) = 'promo'::TEXT
                ) THEN 'Promo Compliance'::TEXT
                WHEN (
                    upper((itg_mds_kr_ps_weights.kpi)::TEXT) = 'Share of Assortment'::TEXT
                ) THEN 'soa compliance'::TEXT
                WHEN (
                    upper((itg_mds_kr_ps_weights.kpi)::TEXT) = 'Share of Shelf'::TEXT
                ) THEN 'sos compliance'::TEXT
                ELSE upper((itg_mds_kr_ps_weights.kpi)::TEXT)
            END
        )::CHARACTER VARYING AS kpi,
        itg_mds_kr_ps_weights.channel,
        itg_mds_kr_ps_weights.retail_env AS retail_environment,
        itg_mds_kr_ps_weights.weight
    FROM itg_mds_kr_ps_weights
),
hongkong as
(
    SELECT 
        'Hong Kong' AS market,
        (
            CASE
                WHEN (
                    upper((itg_mds_hk_ps_weights.kpi)::TEXT) = 'OOS Compliance'::TEXT
                ) THEN 'OSA COMPLIANCE'::TEXT
                WHEN (
                    upper((itg_mds_hk_ps_weights.kpi)::TEXT) = 'Planogram'::TEXT
                ) THEN 'Planogram compliance'::TEXT
                WHEN (
                    upper((itg_mds_hk_ps_weights.kpi)::TEXT) = 'promo'::TEXT
                ) THEN 'Promo Compliance'::TEXT
                WHEN (
                    upper((itg_mds_hk_ps_weights.kpi)::TEXT) = 'Share of Assortment'::TEXT
                ) THEN 'soa compliance'::TEXT
                WHEN (
                    upper((itg_mds_hk_ps_weights.kpi)::TEXT) = 'Share of Shelf'::TEXT
                ) THEN 'sos compliance'::TEXT
                ELSE upper((itg_mds_hk_ps_weights.kpi)::TEXT)
            END
        )::CHARACTER VARYING AS kpi,
        itg_mds_hk_ps_weights.channel,
        itg_mds_hk_ps_weights.retail_env AS retail_environment,
        itg_mds_hk_ps_weights.weight
    FROM itg_mds_hk_ps_weights
),
japan as
(
    SELECT 
        'Japan' AS market,
        (
            CASE
                WHEN (
                    upper((itg_mds_jp_ps_weights.kpi)::TEXT) = 'OOS Compliance'::TEXT
                ) THEN 'OSA COMPLIANCE'::TEXT
                WHEN (
                    upper((itg_mds_jp_ps_weights.kpi)::TEXT) = 'Planogram'::TEXT
                ) THEN 'Planogram compliance'::TEXT
                WHEN (
                    upper((itg_mds_jp_ps_weights.kpi)::TEXT) = 'promo'::TEXT
                ) THEN 'Promo Compliance'::TEXT
                WHEN (
                    upper((itg_mds_jp_ps_weights.kpi)::TEXT) = 'Share of Assortment'::TEXT
                ) THEN 'soa compliance'::TEXT
                WHEN (
                    upper((itg_mds_jp_ps_weights.kpi)::TEXT) = 'Share of Shelf'::TEXT
                ) THEN 'sos compliance'::TEXT
                ELSE upper((itg_mds_jp_ps_weights.kpi)::TEXT)
            END
        )::CHARACTER VARYING AS kpi,
        itg_mds_jp_ps_weights.channel,
        itg_mds_jp_ps_weights.retail_env AS retail_environment,
        itg_mds_jp_ps_weights.weight
    FROM itg_mds_jp_ps_weights
),
india as
(
    SELECT 
        'India' AS market,
        (
            CASE
                WHEN (
                    upper((itg_mds_in_ps_weights.kpi)::TEXT) = 'OOS Compliance'::TEXT
                ) THEN 'OSA COMPLIANCE'::TEXT
                WHEN (
                    upper((itg_mds_in_ps_weights.kpi)::TEXT) = 'Planogram'::TEXT
                ) THEN 'Planogram compliance'::TEXT
                WHEN (
                    upper((itg_mds_in_ps_weights.kpi)::TEXT) = 'promo'::TEXT
                ) THEN 'Promo Compliance'::TEXT
                WHEN (
                    upper((itg_mds_in_ps_weights.kpi)::TEXT) = 'Share of Assortment'::TEXT
                ) THEN 'soa compliance'::TEXT
                WHEN (
                    upper((itg_mds_in_ps_weights.kpi)::TEXT) = 'Share of Shelf'::TEXT
                ) THEN 'sos compliance'::TEXT
                ELSE upper((itg_mds_in_ps_weights.kpi)::TEXT)
            END
        )::CHARACTER VARYING AS kpi,
        itg_mds_in_ps_weights.channel,
        itg_mds_in_ps_weights.retail_env AS retail_environment,
        itg_mds_in_ps_weights.weight
    FROM itg_mds_in_ps_weights
),
pacific as
(
    SELECT 
        itg_mds_pacific_ps_weights.market,
        (
            CASE
                WHEN (
                    upper((itg_mds_pacific_ps_weights.kpi)::TEXT) = 'OOS Compliance'::TEXT
                ) THEN 'OSA COMPLIANCE'::TEXT
                WHEN (
                    upper((itg_mds_pacific_ps_weights.kpi)::TEXT) = 'Planogram'::TEXT
                ) THEN 'Planogram compliance'::TEXT
                WHEN (
                    upper((itg_mds_pacific_ps_weights.kpi)::TEXT) = 'promo'::TEXT
                ) THEN 'Promo Compliance'::TEXT
                WHEN (
                    upper((itg_mds_pacific_ps_weights.kpi)::TEXT) = 'Share of Assortment'::TEXT
                ) THEN 'soa compliance'::TEXT
                WHEN (
                    upper((itg_mds_pacific_ps_weights.kpi)::TEXT) = 'Share of Shelf'::TEXT
                ) THEN 'sos compliance'::TEXT
                ELSE upper((itg_mds_pacific_ps_weights.kpi)::TEXT)
            END
        )::CHARACTER VARYING AS kpi,
        itg_mds_pacific_ps_weights.channel,
        itg_mds_pacific_ps_weights.retail_env AS retail_environment,
        itg_mds_pacific_ps_weights.weight
    FROM itg_mds_pacific_ps_weights
),
final as 
(
        select * from indonesia
        union all
        select * from vietnam
        union all
        select * from thailand
        union all
        select * from philippines
        union all
        select * from singapore
        union all
        select * from malaysia
        union all
        select * from taiwan
        union all
        select * from korea
        union all
        select * from hongkong
        union all
        select * from japan
        union all
        select * from india
        union all
        select * from pacific
)
select * from final