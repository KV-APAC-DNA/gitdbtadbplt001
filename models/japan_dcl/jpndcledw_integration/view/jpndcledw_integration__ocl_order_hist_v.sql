with ocl_main_data_v as (
select * from {{ ref('jpndcledw_integration__ocl_main_data_v') }}
),

tbecorder as (
select * from {{ ref('jpndclitg_integration__tbecorder') }}
),

tbecordermeisai as (
select * from {{ ref('jpndclitg_integration__tbecordermeisai') }}
),

c_tbeckesai as (
select * from {{ ref('jpndclitg_integration__c_tbeckesai') }}
),

hanyo_attr as (
select * from {{ ref('jpndcledw_integration__hanyo_attr') }}
),
tbecsalesroutemst as (
select * from {{ ref('jpndclitg_integration__tbecsalesroutemst') }}
),

final as (
SELECT '0' AS sort_key,
    '顧客ID' AS customer_id,
    '受注日' AS order_date,
    '受注ID' AS order_id,
    '決済' AS payment,
    '受注経路' AS order_route,
    '請求金額' AS invice_amount,
    '最短お届け日' AS earlst_delivery_date,
    '最短お届け日時間帯' AS earlst_delivery_time,
    '指定配送' AS dsgnd_delivery_date,
    '指定配送時間帯' AS dsgnd_delivery_time,
    '登録日' AS register_date,
    '登録時刻' AS register_time,
    '小計' AS sub_total,
    '商品ID' AS item_id,
    '商品名' AS item_name,
    '数量' AS quantity

UNION ALL

SELECT '1' AS sort_key,
    (
        (
            (
                '"'::TEXT || lpad("replace" (
                        (ocl_main.CUSTOMER_ID)::TEXT,
                        '"'::TEXT,
                        ''::TEXT
                        ), 10, (0)::TEXT)
                ) || '"'::TEXT
            )
        )::CHARACTER VARYING AS customer_id,
    ((('"'::TEXT || to_char(odr.dsorderdt, 'YYYYMMDD'::TEXT)) || '"'::TEXT))::CHARACTER VARYING AS order_date,
    '""' AS order_id,
    '""' AS payment,
    ((('"'::TEXT || (sales_rt.dsroutename)::TEXT) || '"'::TEXT))::CHARACTER VARYING AS order_route,
    '""' AS invice_amount,
    '""' AS earlst_delivery_date,
    '""' AS earlst_delivery_time,
    ((('"'::TEXT || COALESCE(to_char(kesai.c_dstodokedate, 'YYYYMMDD'::TEXT), ''::TEXT)) || '"'::TEXT))::CHARACTER VARYING AS dsgnd_delivery_date,
    ((('"'::TEXT || (COALESCE(desgnd_dlvrytm.attr2, ''::CHARACTER VARYING))::TEXT) || '"'::TEXT))::CHARACTER VARYING AS dsgnd_delivery_time,
    '""' AS register_date,
    '""' AS register_time,
    '""' AS sub_total,
    ((('"'::TEXT || (COALESCE(meisai.dsitemid, ''::CHARACTER VARYING))::TEXT) || '"'::TEXT))::CHARACTER VARYING AS item_id,
    ((('"'::TEXT || (COALESCE(meisai.dsitemname, ''::CHARACTER VARYING))::TEXT) || '"'::TEXT))::CHARACTER VARYING AS item_name,
    '""' AS quantity
FROM (
    (
        (
            (
                (
                    ocl_main_data_v ocl_main JOIN tbecorder odr ON (
                            (
                                (
                                    "replace" (
                                        (ocl_main.customer_id)::TEXT,
                                        '"'::TEXT,
                                        ''::TEXT
                                        )
                                    )::BIGINT = odr.diecusrid
                                )
                            )
                    ) JOIN tbecordermeisai meisai ON ((odr.diorderid = meisai.diorderid))
                ) JOIN c_tbeckesai kesai ON ((kesai.diorderid = odr.diorderid))
            ) JOIN hanyo_attr desgnd_dlvrytm ON (
                (
                    ((kesai.c_dsdeliverytime)::TEXT = (desgnd_dlvrytm.attr1)::TEXT)
                    AND ((desgnd_dlvrytm.kbnmei)::TEXT = 'deliverytime'::TEXT)
                    )
                )
        ) JOIN tbecsalesroutemst sales_rt ON ((odr.dirouteid = sales_rt.dirouteid))
    )
WHERE (
        (
            (
                (
                    (
                        (
                            (
                                (
                                    ((ocl_main.sort_key)::TEXT = '1'::TEXT)
                                    AND (
                                        (to_char(odr.dsuriagedt, 'YYYYMMDD'::TEXT) >= to_char(add_months(convert_timezone('Asia/Tokyo'::TEXT, (current_timestamp()::TEXT)::TIMESTAMP without TIME zone), (- 12)::BIGINT), 'YYYYMMDD'::TEXT))
                                        AND (to_char(odr.dsuriagedt, 'YYYYMMDD'::TEXT) <= to_char(convert_timezone('Asia/Tokyo'::TEXT, (current_timestamp()::TEXT)::TIMESTAMP without TIME zone), 'YYYYMMDD'::TEXT))
                                        )
                                    )
                                AND (odr.diseikyuprc > 0)
                                )
                            AND ((odr.dicancel)::TEXT = '0'::TEXT)
                            )
                        AND ((odr.dielimflg)::TEXT = '0'::TEXT)
                        )
                    AND (meisai.diusualprc > 0)
                    )
                AND ((meisai.dicancel)::TEXT = '0'::TEXT)
                )
            AND ((meisai.dielimflg)::TEXT = '0'::TEXT)
            )
        AND (meisai.c_dikesaiid = kesai.c_dikesaiid)
        )
ORDER BY 1)


select * from final