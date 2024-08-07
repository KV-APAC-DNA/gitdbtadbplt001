with edi_cstm_rlt_m as(
    select * from {{ ref('jpnedw_integration__edi_cstm_rlt_m') }}
),
transformed as(
    SELECT edi_cstm_rlt_m.create_dt
        ,edi_cstm_rlt_m.create_user
        ,edi_cstm_rlt_m.update_dt
        ,edi_cstm_rlt_m.update_user
        ,edi_cstm_rlt_m.sold_to_cstm
        ,edi_cstm_rlt_m.ship_to_cstm
        ,edi_cstm_rlt_m.bill_to_cstm
        ,edi_cstm_rlt_m.pay_cstm
    FROM edi_cstm_rlt_m
)
select * from transformed