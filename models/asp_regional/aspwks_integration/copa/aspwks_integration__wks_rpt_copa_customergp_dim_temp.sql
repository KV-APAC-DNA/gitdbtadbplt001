with wks_rpt_copa_customergp_base as
(
    select * from {{ ref('aspwks_integration__wks_rpt_copa_customergp_base') }}
),
final as
(
    SELECT distinct
    ctry_nm,
    "cluster",
    sls_org,
    Prft_ctr,
    obj_crncy_co_obj,
    from_crncy,
    to_crncy,
    matl_num,
    cust_num 
    from wks_rpt_copa_customergp_base
)
select * from final