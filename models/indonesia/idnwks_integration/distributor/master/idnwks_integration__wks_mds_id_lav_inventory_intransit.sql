with itg_mds_id_lav_inventory_intransit as
(
    select * from idnitg_integration.itg_mds_id_lav_inventory_intransit
),
sdl_mds_id_lav_inventory_intransit as
(
    select * from {{ source('idnsdl_raw', 'sdl_mds_id_lav_inventory_intransit') }}
),
temp_a as 
(
    SELECT *
FROM itg_mds_id_lav_inventory_intransit
WHERE NVL(
        LEFT (TO_CHAR(CREATED_ON1, 'YYYYMMDD'), 6),
        '99991231'
    ) || month NOT IN (
        SELECT DISTINCT NVL(
                LEFT (
                    TO_CHAR(CAST("created on1" AS DATE), 'YYYYMMDD'),
                    6
                ),
                '99991231'
            ) || month
        FROM sdl_mds_id_lav_inventory_intransit)
),
temp_b as 
(
    select month as month,
    plant as plant,
    saty as saty,
    document as doc_num,
    "po number" as po_num,
    material as matl_num,
    description as matl_desc,
    "ship-to" as ship_to,
    rj as rj_cd,
    reason as rj_reason,
    billing as billing,
    "not invoiced" as not_invoiced,
    "DOC. DATE" as doc_date,
    "goods issue" as goods_issue,
    "billing date" as bill_date,
    rdd as rdd,
    "order qty" as order_qty,
    "confirm qty" as confirmed_qty,
    "net value" as net_value,
    "billing check" as billing_check,
    "order value" as order_value,
    "billing value" as billing_value,
    unrecoverable as unrecoverable_ord_val,
    "open orders" as open_orders_val,
    "return value" as return_value,
    "customer type" as cust_type,
    "customer group" as cust_grp,
    customer as cust_name,
    "bill month" as bill_month,
    "return billing" as return_billing,
    "unrecoverable billing" as unrecoverable_billing,
    "ship-to 2" as ship_to_2,
    "gi date/rdd" as gi_date,
    "order week" as order_week,
    pod as pod,
    cast("1st day" as date) as first_day,
    remarks,
    name1,
    cast("created on1" as date) as created_on1,
    md5(
                      nvl (month, '') || nvl (plant, '') || nvl (saty, '') || nvl (document, '') || nvl ("po number", '') || nvl (material, '') || nvl (description, '') || nvl ("ship-to", '') || nvl (rj, '') || nvl (reason, '') || nvl (billing, '') || nvl ("not invoiced", '') || nvl ("DOC. DATE", '9999-12-31') || nvl ("goods issue", '9999-12-31') || nvl ("billing date", '9999-12-31') || nvl (rdd, '9999-12-31') || nvl ("order qty", 0) || nvl ("confirm qty", 0) || nvl ("net value", 0) || nvl ("billing check", 0) || nvl ("order value", 0) || nvl ("billing value", 0) || nvl (unrecoverable, 0) || nvl ("open orders", 0) || nvl ("return value", 0) || nvl ("customer type", '') || nvl ("customer group", '') || nvl (customer, '') || nvl ("bill month", '') || nvl ("return billing", 0) || nvl ("unrecoverable billing", 0) || nvl ("ship-to 2", '') || nvl ("gi date/rdd", '9999-12-31') || nvl ("order week", '') || nvl (pod, '9999-12-31') || nvl (cast("1st day" as date), '9999-12-31') || nvl (remarks, '') || nvl (name1, '') || nvl (cast("created on1" as date), '9999-12-31')
    ) AS HASHKEY,
    convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) AS crtd_dttm
FROM sdl_mds_id_lav_inventory_intransit
),
final as 
(
    select * from temp_a 
    union all
    select * from temp_b
)
select * from final