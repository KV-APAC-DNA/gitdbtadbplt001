WITH report_006_a
AS (
    select * from dev_dna_core.snapjpdcledw_integration.report_006_a
    ),
final
AS (
    SELECT channel_name::VARCHAR(6) AS channel_name,
        channel_id::VARCHAR(6) AS channel_id,
        yymm::VARCHAR(100) AS yymm,
        "ユニーク契約者数"::NUMBER(38, 0) AS "ユニーク契約者数",
        CAST(CONVERT_TIMEZONE('Asia/Tokyo', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))) AS DATE) AS rec_insertdate
    FROM report_006_a
    )
SELECT * FROM final