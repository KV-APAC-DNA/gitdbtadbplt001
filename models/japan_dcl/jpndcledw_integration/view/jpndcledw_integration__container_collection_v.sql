with odr_receive_header_v as (
 select * from dev_dna_core.snapjpdcledw_integration.odr_receive_header_v
)
,
odr_receive_detail_v as (
select * from dev_dna_core.snapjpdcledw_integration.odr_receive_detail_v
),
tbecorder as (
select * from dev_dna_core.snapjpdclitg_integration.tbecorder
),
container_collection_exclude_no as (
select * from dev_dna_core.snapjpdcledw_integration.container_collection_exclude_no
)
,
base_data
AS (
    SELECT '1' AS sort_key,
        (h.customerid)::CHARACTER VARYING AS customer_id,
        '' AS point_issuing_id,
        (translate(h.dsordermemo, ('０１２３４５６７８９'::CHARACTER VARYING)::TEXT, ('0123456789'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS points,
        (to_char(date_add(('year'::CHARACTER VARYING)::TEXT, (1)::BIGINT, convert_timezone(('JST'::CHARACTER VARYING)::TEXT, ('now'::CHARACTER VARYING)::TIMESTAMP without TIME zone)), ('YYYY/MM'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS point_expiration_date,
        '容器回収付与ポイント' AS administrative_memo,
        (to_char(h.orderreceivedate, ('YYYYMMDD'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS order_date,
        (to_char(o.dsren, ('yyyymmdd'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS last_updated_date,
        CASE 
            WHEN ((o.dishukkasts)::TEXT = ('9910'::CHARACTER VARYING)::TEXT)
                THEN '出荷対象外'::CHARACTER VARYING
            WHEN ((o.dishukkasts)::TEXT = ('9010'::CHARACTER VARYING)::TEXT)
                THEN '出荷保留'::CHARACTER VARYING
            WHEN ((o.dishukkasts)::TEXT = ('1010'::CHARACTER VARYING)::TEXT)
                THEN '未出荷'::CHARACTER VARYING
            WHEN ((o.dishukkasts)::TEXT = ('1020'::CHARACTER VARYING)::TEXT)
                THEN '受注確定済'::CHARACTER VARYING
            WHEN ((o.dishukkasts)::TEXT = ('1030'::CHARACTER VARYING)::TEXT)
                THEN '一部出荷指示済'::CHARACTER VARYING
            WHEN ((o.dishukkasts)::TEXT = ('1040'::CHARACTER VARYING)::TEXT)
                THEN '出荷指示済'::CHARACTER VARYING
            WHEN ((o.dishukkasts)::TEXT = ('1050'::CHARACTER VARYING)::TEXT)
                THEN '一部出荷済'::CHARACTER VARYING
            WHEN ((o.dishukkasts)::TEXT = ('1060'::CHARACTER VARYING)::TEXT)
                THEN '出荷済'::CHARACTER VARYING
            WHEN ((o.dishukkasts)::TEXT = ('5010'::CHARACTER VARYING)::TEXT)
                THEN '与信待未出荷'::CHARACTER VARYING
            WHEN ((o.dishukkasts)::TEXT = ('5020'::CHARACTER VARYING)::TEXT)
                THEN '与信待出荷保留'::CHARACTER VARYING
            ELSE NULL::CHARACTER VARYING
            END AS shipment_status,
        (h.odrreceiveno)::CHARACTER VARYING AS order_id
    FROM (
        (
            odr_receive_header_v h JOIN odr_receive_detail_v d ON ((h.odrreceiveno = d.odrreceiveno))
            ) JOIN TBECORDER o ON ((h.odrreceiveno = o.diorderid))
        )
    WHERE (
            (
                (
                    (
                        (
                            ((d.itemcode)::TEXT = ('9200001600'::CHARACTER VARYING)::TEXT)
                            AND (
                                (
                                    ((h.orderreceivetype)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
                                    OR ((h.orderreceivetype)::TEXT = ('1'::CHARACTER VARYING)::TEXT)
                                    )
                                OR ((h.orderreceivetype)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
                                )
                            )
                        AND (
                            NOT (
                                (h.odrreceiveno)::CHARACTER VARYING(25) IN (
                                    SELECT DISTINCT container_collection_exclude_no.odrreceiveno
                                    FROM CONTAINER_COLLECTION_EXCLUDE_NO
                                    )
                                )
                            )
                        )
                    AND ((h.cancelflag)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
                    )
                AND (h.grantpoint IS NOT NULL)
                )
            AND (COALESCE(h.grantpoint, (0)::BIGINT) < 1001)
            )
    ),
base_data1
AS (
    SELECT '1' AS sort_key,
        (h.customerid)::CHARACTER VARYING AS customer_id,
        '' AS point_issuing_id,
        (translate(h.dsordermemo, ('０１２３４５６７８９'::CHARACTER VARYING)::TEXT, ('0123456789'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS points,
        (to_char(date_add(('year'::CHARACTER VARYING)::TEXT, (1)::BIGINT, convert_timezone(('JST'::CHARACTER VARYING)::TEXT, ('now'::CHARACTER VARYING)::TIMESTAMP without TIME zone)), ('YYYY/MM'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS point_expiration_date,
        '容器回収付与ポイント' AS administrative_memo,
        (to_char(h.orderreceivedate, ('YYYYMMDD'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS order_date,
        (to_char(o.dsren, ('yyyymmdd'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS last_updated_date,
        CASE 
            WHEN ((o.dishukkasts)::TEXT = ('9910'::CHARACTER VARYING)::TEXT)
                THEN '出荷対象外'::CHARACTER VARYING
            WHEN ((o.dishukkasts)::TEXT = ('9010'::CHARACTER VARYING)::TEXT)
                THEN '出荷保留'::CHARACTER VARYING
            WHEN ((o.dishukkasts)::TEXT = ('1010'::CHARACTER VARYING)::TEXT)
                THEN '未出荷'::CHARACTER VARYING
            WHEN ((o.dishukkasts)::TEXT = ('1020'::CHARACTER VARYING)::TEXT)
                THEN '受注確定済'::CHARACTER VARYING
            WHEN ((o.dishukkasts)::TEXT = ('1030'::CHARACTER VARYING)::TEXT)
                THEN '一部出荷指示済'::CHARACTER VARYING
            WHEN ((o.dishukkasts)::TEXT = ('1040'::CHARACTER VARYING)::TEXT)
                THEN '出荷指示済'::CHARACTER VARYING
            WHEN ((o.dishukkasts)::TEXT = ('1050'::CHARACTER VARYING)::TEXT)
                THEN '一部出荷済'::CHARACTER VARYING
            WHEN ((o.dishukkasts)::TEXT = ('1060'::CHARACTER VARYING)::TEXT)
                THEN '出荷済'::CHARACTER VARYING
            WHEN ((o.dishukkasts)::TEXT = ('5010'::CHARACTER VARYING)::TEXT)
                THEN '与信待未出荷'::CHARACTER VARYING
            WHEN ((o.dishukkasts)::TEXT = ('5020'::CHARACTER VARYING)::TEXT)
                THEN '与信待出荷保留'::CHARACTER VARYING
            ELSE NULL::CHARACTER VARYING
            END AS shipment_status,
        (h.odrreceiveno)::CHARACTER VARYING AS order_id
    FROM (
        (
            odr_receive_header_v h JOIN odr_receive_detail_v d ON ((h.odrreceiveno = d.odrreceiveno))
            ) JOIN TBECORDER o ON ((h.odrreceiveno = o.diorderid))
        )
    WHERE (
            (
                (
                    (
                        (
                            ((d.itemcode)::TEXT = ('9200001600'::CHARACTER VARYING)::TEXT)
                            AND (
                                (
                                    ((h.orderreceivetype)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
                                    OR ((h.orderreceivetype)::TEXT = ('1'::CHARACTER VARYING)::TEXT)
                                    )
                                OR ((h.orderreceivetype)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
                                )
                            )
                        AND (
                            NOT (
                                (h.odrreceiveno)::CHARACTER VARYING(25) IN (
                                    SELECT DISTINCT container_collection_exclude_no.odrreceiveno
                                    FROM CONTAINER_COLLECTION_EXCLUDE_NO
                                    )
                                )
                            )
                        )
                    AND ((h.cancelflag)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
                    )
                AND (h.grantpoint IS NOT NULL)
                )
            AND (COALESCE(h.grantpoint, (0)::BIGINT) < 1001)
            )
    ),

final
AS (
    SELECT '0' AS sort_key,
        '顧客ID' AS customer_id,
        'ポイント発行ID' AS point_issuing_id,
        'ポイント数' AS points,
        'ポイント有効期限' AS point_expiration_date,
        '管理メモ' AS administrative_memo,
        '受注日' AS order_date,
        '最終更新日' AS last_updated_date,
        '出荷ステータス' AS shipment_status,
        '受注ID'::CHARACTER VARYING AS order_id
    
    UNION ALL
    
    SELECT base_data.sort_key,
        base_data.customer_id,
        base_data.point_issuing_id,
        base_data.points,
        base_data.point_expiration_date,
        base_data.administrative_memo,
        base_data.order_date,
        base_data.last_updated_date,
        base_data.shipment_status,
        base_data.order_id
    FROM base_data
    
    UNION ALL
    
    SELECT '2' AS sort_key,
        '本日データレコードなし' AS customer_id,
        NULL AS point_issuing_id,
        NULL AS points,
        NULL AS point_expiration_date,
        NULL AS administrative_memo,
        NULL AS order_date,
        NULL AS last_updated_date,
        NULL AS shipment_status,
        NULL AS order_id
    where (
            (
                select count(*) as count
                from base_data1
                ) = 0
            )
    order by 1
    )

select * from final