with kesai_h_data_mart_mv as(
    select * from {{ ref('jpndcledw_integration__kesai_h_data_mart_mv') }}
),
cim01kokya as(
    select * from  {{ ref('jpndcledw_integration__cim01kokya') }}
),
kesai_m_data_mart_mv as(
    select * from {{ ref('jpndcledw_integration__kesai_m_data_mart_mv') }}
),
transformed as(
    SELECT DISTINCT h.kokyano,
	h.saleno
    FROM kesai_h_data_mart_mv h
    INNER JOIN cim01kokya c ON h.kokyano = c.kokyano
    INNER JOIN kesai_m_data_mart_mv m ON h.saleno_key = m.saleno_key
)
select * from transformed