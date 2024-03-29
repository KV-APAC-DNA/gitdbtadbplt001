with source as(
    select * from {{ ref('vnmitg_integration__itg_vn_mt_pos_cust_master') }}
),
transformed as(
    select 
        source.chain
        ,source.customer_name
        ,source.customer_store_code
        ,source.district
        ,source.note_closed_store
        ,source.plant
        ,source.STATUS
        ,source.store_code
        ,source.store_name
        ,source.store_name_2
        ,source.wh
        ,source.zone
    from source
    where ((source.active)::text = ('Y'::character varying)::text)
)
select * from transformed