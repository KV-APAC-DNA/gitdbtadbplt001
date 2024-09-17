with vw_dmnd_frcst_customer_dim as(
    select * from {{ ref('pcfedw_integration__vw_dmnd_frcst_customer_dim') }}
),
transformered as
(
    select * from vw_dmnd_frcst_customer_dim
)
select * from transformered