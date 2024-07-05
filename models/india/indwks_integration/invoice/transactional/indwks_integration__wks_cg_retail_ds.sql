with 
edw_product_dim as 
(
    select * from {{ ref('indedw_integration__edw_product_dim') }}
),
wks_cg_retail_curr_prev as 
(
    select * from {{ ref('indwks_integration__wks_cg_retail_curr_prev') }}
) ,
final as 
(
    SELECT 
        pf.Month,
        pf.Year,
        pf.Customer_Code,
        pf.Product_Code,
        sum(ifnull(pf.Sec_Prd_Qty,0) + ifnull(pf.Sec_Prd_Qty_Ret,0) + ifnull(pf.Sec_Late_Prd_Qty,0) + ifnull
        (pf.Sec_Late_Prd_Qty_Ret,0)) AS Retail_Quantity,
        sum (ifnull(pf.Sec_Prd_NR_Value,0)  + ifnull(pf.Sec_Prd_NR_Value_Ret,0) + ifnull(pf.Sec_Late_Prd_NR_Value,0) + 
        ifnull(pf.Sec_Late_Prd_NR_Value_Ret,0)) as Retail_Value,
        sum(((ifnull(pf.Sec_Prd_Qty,0) + ifnull(pf.Sec_Prd_Qty_Ret,0) + ifnull(pf.Sec_Late_Prd_Qty,0) + ifnull
        (pf.Sec_Late_Prd_Qty_Ret,0))  *  ifnull( pdim.Net_Wt,0))/1000) as Wt_Ret_Qty,
        current_timestamp() as updt_dttm
        FROM wks_cg_retail_curr_prev pf
        LEFT JOIN ( select product_code, net_wt from edw_product_dim ) pdim
        ON pf.product_code  = pdim.product_code 
        group by Month, Year,Customer_Code,pf.Product_Code 
    
)
select * from final
