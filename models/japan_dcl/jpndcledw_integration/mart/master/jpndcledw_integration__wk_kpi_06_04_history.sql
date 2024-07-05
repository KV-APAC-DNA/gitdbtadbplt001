WITH wk_kpi_06_04 AS
(
    -- SELECT * FROM dev_dna_core.snapjpdcledw_integration.wk_kpi_06_04
    select * from {{ source('jpndcledw', 'jpndcledw_integration__wk_kpi_06_04') }}
),

final AS
(
SELECT 
    "大区分"::VARCHAR(200) as "大区分",
	"小区分"::VARCHAR(200) as "小区分",
	yymm::VARCHAR(100) as yymm,
	"販路"::VARCHAR(200) as "販路",
	"総契約件数"::NUMBER(38,0) as "総契約件数",
	"総契約金額"::NUMBER(38,0) as "総契約金額",
	"ユニーク契約者数"::NUMBER(38,0) as "ユニーク契約者数",
    CAST(CONVERT_TIMEZONE('Asia/Tokyo', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))) AS DATE) AS rec_insertdate
FROM wk_kpi_06_04
)

SELECT * FROM final