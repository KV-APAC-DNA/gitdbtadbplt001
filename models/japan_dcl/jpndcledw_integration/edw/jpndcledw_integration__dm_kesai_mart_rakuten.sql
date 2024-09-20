WITH rakutenorderdetail
AS (
    SELECT *
    FROM {{ ref('jpndclitg_integration__rakutenorderdetail') }}
    ),
tbecitem
AS (
    SELECT *
    FROM {{ ref('jpndclitg_integration__tbecitem') }}
    ),
tbecsetitem 
AS (
    SELECT * FROM  {{ ref('jpndclitg_integration__tbecsetitem') }}
    ),
cim01kokya
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__cim01kokya') }}
    ),
cim01
AS (
    SELECT max(kokyano) AS kokyano,
        tel
    FROM (
        SELECT kokyano,
            tel AS tel
        FROM cim01kokya
        
        UNION
        
        SELECT kokyano,
            tel_mobile AS tel
        FROM cim01kokya
        
        UNION
        
        SELECT kokyano,
            tel_other AS tel
        FROM cim01kokya
        )
    GROUP BY tel
    ),
base
AS (
    SELECT diid,
        dsitemid,
        dsitemname
    FROM tbecitem
    ),
item_details
AS (
    SELECT base.dsitemid AS item_no,
        max(t.dsitemid) AS h_o_item_code,
        t.dsitemname AS h_o_item_name
    FROM tbecitem t
    INNER JOIN base ON base.dsitemname = t.dsitemname
    INNER JOIN tbecsetitem ts ON t.diid = ts.diid
    GROUP BY 1,
        3
    ),
rakuten
AS (
    SELECT order_no,
        email_addr,
        order_datetime,
        item_control_no,
        qty,
        item_no,
        total_price_tax10,
        tax10,
        total_price_tax8,
        tax8,
        coupon_tax10,
        coupon_tax8,
        inserted_date,
        updated_date,
        mobile1 || mobile2 || mobile3 AS mobile
    FROM rakutenorderdetail
    ),
final as
(
    SELECT DISTINCT CASE 
            WHEN c.kokyano IS NULL
                THEN r.email_addr
            ELSE c.kokyano
            END AS kokyano,
        r.order_no AS saleno_key,
        r.order_no AS saleno,
        r.order_datetime AS order_dt,
        'rakuten' AS channel,
        '0' AS juchkbn,
        item_details.h_o_item_code,
        item_details.h_o_item_name,
        item_details.h_o_item_code || item_details.h_o_item_name AS h_o_item_cname,
        r.qty AS h_o_item_anbun_qty,
        item_details.h_o_item_code AS h_item_code,
        r.item_no AS z_item_code,
        r.qty AS z_item_suryo,
        (nvl(r.total_price_tax10::INT, 0) - nvl(r.tax10::INT, 0)) + (nvl(r.total_price_tax8::INT, 0) - nvl(r.tax8::INT, 0)) AS gts,
        nvl(r.qty::INT, 0) AS gts_qty,
        (nvl(r.coupon_tax10::INT, 0) + nvl(r.coupon_tax8::INT, 0)) AS ciw_discount,
        0 AS ciw_point,
        0 AS ciw_return,
        0 AS ciw_return_qty,
        gts - ciw_discount AS nts,
        r.inserted_date AS inserted_date,
        'ETL_Batch' AS inserted_by,
        r.updated_date AS updated_date,
        '' AS updated_by
    FROM rakuten r
    LEFT JOIN cim01 c ON r.mobile = c.tel
        AND r.mobile <> '0000000000'
    LEFT JOIN item_details ON item_details.item_no = r.item_no
)
select * from final