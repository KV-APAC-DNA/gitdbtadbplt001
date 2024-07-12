WITH dm_kesai_mart_dly_general
AS
(
    select * from snapjpdcledw_integration.dm_kesai_mart_dly_general
),
i_channel
AS (
	SELECT DISTINCT kokyano,
		first_value(channel IGNORE nulls) OVER (
			PARTITION BY kokyano ORDER BY order_dt rows BETWEEN unbounded preceding
					AND unbounded following
			) AS i_channel
	FROM dm_kesai_mart_dly_general
	WHERE channel IN ('通販', 'Web', '直営・百貨店')
		AND juchkbn IN (0, 1, 2)
		AND MEISAINUKIKINGAKU <> 0
	),
l_channel
AS (
	SELECT DISTINCT kokyano,
		last_value(channel IGNORE nulls) OVER (
			PARTITION BY kokyano ORDER BY order_dt rows BETWEEN unbounded preceding
					AND unbounded following
			) AS l_channel
	FROM dm_kesai_mart_dly_general
	WHERE channel IN ('通販', 'Web', '直営・百貨店')
		AND juchkbn IN (0, 1, 2)
		AND MEISAINUKIKINGAKU <> 0
	),
final as
(
SELECT DISTINCT dkmd.kokyano::VARCHAR(60) AS Customer_No,
	i_channel.i_channel::VARCHAR(60) AS i_channel,
	l_channel.l_channel::VARCHAR(60) AS l_channel
FROM dm_kesai_mart_dly_general dkmd
LEFT JOIN i_channel ON dkmd.kokyano = i_channel.kokyano
LEFT JOIN l_channel ON dkmd.kokyano = l_channel.kokyano
WHERE dkmd.channel IN ('通販', 'Web', '直営・百貨店')
	AND dkmd.order_dt >= '2019-01-01'
	AND (
		dkmd.juchkbn::TEXT = 0::CHARACTER VARYING::TEXT
		OR dkmd.juchkbn::TEXT = 1::CHARACTER VARYING::TEXT
		OR dkmd.juchkbn::TEXT = 2::CHARACTER VARYING::TEXT
		)
	AND dkmd.meisainukikingaku <> 0::NUMERIC::NUMERIC(18, 0)
)
select * from final