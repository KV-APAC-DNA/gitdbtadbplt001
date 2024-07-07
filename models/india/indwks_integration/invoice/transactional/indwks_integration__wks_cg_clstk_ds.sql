with 
itg_pf_clstk_mth_ds as 
(
    select * from {{ ref('inditg_integration__itg_pf_clstk_mth_ds') }}
),
edw_product_dim as 
(
    select * from {{ ref('indedw_integration__edw_product_dim') }}
),
final as 
(
    SELECT
        src.month,
        src.year,
        src.customer_code,
        src.product_code,
        src.Cls_Stock_Qty,
        src.Cl_Stck_Value,
        src.Wt_Stock_Qty,
        current_timestamp() as updt_dttm,
        current_timestamp() as crt_dttm,
        null as chng_flg
        FROM
        (
        select
        pf.Month as month,
        pf.Year as year,
        pf.Customer_Code as customer_code,
        pf.Product_Code as product_code,
        Sum(pf.cl_stck_qty) as Cls_Stock_Qty ,
        Sum(pf.cl_stck_value) as Cl_Stck_Value,
        sum((ifnull(pf.cl_stck_qty,0) * ifnull(pdim.Net_Wt,0) )/ 1000)   as  Wt_stock_Qty
        from itg_pf_clstk_mth_ds pf
        LEFT JOIN ( select product_code, net_wt from edw_product_dim ) pdim
        ON pf.product_code  = pdim.product_code
        group by pf.Month,pf.Year,pf.Customer_Code,pf.Product_Code
        )src
        )
select * from final