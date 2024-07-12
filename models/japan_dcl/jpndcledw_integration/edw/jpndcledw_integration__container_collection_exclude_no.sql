{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        unique_key= ["distributor_code","sales_repcode","retailer_code","surveydate","aq_name","link"],
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}} container_collection_exclude_no
                WHERE odrreceiveno  IN (
                SELECT order_ID
                FROM DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CONTAINER_COLLECTION_V ccv
                WHERE CCV.order_id  = container_collection_exclude_no.odrreceiveno 
                )
                AND TO_CHAR(convert_timezone('JST', insert_date),'YYYYMMDD') = TO_CHAR(convert_timezone('JST', sysdate()),'YYYYMMDD');
        {% endif %}"
    )
}}

with CONTAINER_COLLECTION_V as (
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CONTAINER_COLLECTION_V
),
transformed as (
select
order_ID, 
convert_timezone('JST', sysdate()) AS insert_date
FROM container_collection_v 
WHERE shipment_status = '出荷対象外' AND  sort_key ='1'
AND NOT EXISTS
(
SELECT * FROM {{this}} container_collection_exclude_no
WHERE container_collection_exclude_no.ODRRECEIVENO = container_collection_v.order_ID
)
),
final as (
SELECT
order_ID::VARCHAR(25) as odrreceiveno,
insert_date::TIMESTAMP_NTZ(9) as insert_date
from transformed
)
select * from final
