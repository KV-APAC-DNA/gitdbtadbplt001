with source as(
    select * from {{ ref('aspedw_integration__edw_billing_fact') }} 
),
transformed as(
    select 'VN' as cntry_key
        ,'Vietnam' as cntry_nm
        ,source.bill_dt
        ,ltrim((source.bill_num)::text, ('0'::character varying)::text) as bill_num
        ,ltrim((source.bill_item)::text, ('0'::character varying)::text) as bill_item
        ,source.bill_type
        ,ltrim((source.doc_num)::text, ('0'::character varying)::text) as sls_doc_num
        ,ltrim((source.s_ord_item)::text, ('0'::character varying)::text) as sls_doc_item
        ,source.doc_currcy as doc_curr
        ,source.doc_categ as sd_doc_catgy
        ,ltrim((source.sold_to)::text, ('0'::character varying)::text) as sold_to
        ,ltrim((source.ship_to)::text, ('0'::character varying)::text) as ship_to
        ,ltrim((source.material)::text, ('0'::character varying)::text) as matl_num
        ,source.sls_org
        ,abs(source.exchg_rate) as exchg_rate
        ,sum(source.bill_qty) as bill_qty_pc
        ,sum(source.subtotal_1) as grs_trd_sls
        ,sum(source.subtotal_2) as subtotal_2
        ,sum(source.subtotal_3) as subtotal_3
        ,sum(source.subtotal_4) as subtotal_4
        ,sum(source.subtotal_5) as net_amt
        ,sum(source.subtotal_6) as est_nts
        ,sum(source.netval_inv) as net_val
        ,sum(source.gross_val) as gross_val
    from source
    where ((source.sls_org)::text = ('260S'::character varying)::text)
    group by source.bill_dt
        ,source.bill_num
        ,source.bill_item
        ,source.bill_type
        ,ltrim((source.doc_num)::text, ('0'::character varying)::text)
        ,ltrim((source.s_ord_item)::text, ('0'::character varying)::text)
        ,source.doc_currcy
        ,source.doc_categ
        ,source.sold_to
        ,source.ship_to
        ,ltrim((source.material)::text, ('0'::character varying)::text)
        ,source.sls_org
        ,abs(source.exchg_rate)

)

select * from transformed