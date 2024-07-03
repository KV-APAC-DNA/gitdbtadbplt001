with 
itg_pf_retail_mth_ds as 
(
    select * from {{ ref('inditg_integration__itg_pf_retail_mth_ds') }}
),
final  as 
(
Select 
year, 
month, 
customer_code, 
product_code,
sum(Sec_Prd_Qty)Sec_Prd_Qty,
sum(Sec_Prd_Qty_Ret)Sec_Prd_Qty_Ret,
sum(Sec_Prd_NR_Value)Sec_Prd_NR_Value,
sum( Sec_Prd_NR_Value_Ret)Sec_Prd_NR_Value_Ret,
sum(Sec_Late_Prd_Qty)Sec_Late_Prd_Qty,
sum(Sec_Late_Prd_Qty_Ret)Sec_Late_Prd_Qty_Ret,
sum(Sec_Late_Prd_NR_Value)Sec_Late_Prd_NR_Value,
sum(Sec_Late_Prd_NR_Value_Ret)Sec_Late_Prd_NR_Value_Ret
FROM(
                select year,month,ifnull(customer_code,'-1') customer_code,ifnull(product_code,'-1') product_code,
                sum(Sec_Prd_Qty)Sec_Prd_Qty,sum(Sec_Prd_Qty_Ret)Sec_Prd_Qty_Ret,sum(Sec_Prd_NR_Value)Sec_Prd_NR_Value,sum( Sec_Prd_NR_Value_Ret)Sec_Prd_NR_Value_Ret,
                0 Sec_Late_Prd_Qty, 0 Sec_Late_Prd_Qty_Ret, 0 Sec_Late_Prd_NR_Value, 0 Sec_Late_Prd_NR_Value_Ret
                from itg_pf_retail_mth_ds 
                group by year,month,ifnull(customer_code,'-1'),ifnull(product_code,'-1')
                Union all   
                select 
                case when month=12 then year+1 else year end as year,
                case when month=12 then 1 else month+1 end as month,
                ifnull(customer_code,'-1') customer_code,ifnull(product_code,'-1') product_code,
                0 Sec_Prd_Qty, 0 Sec_Prd_Qty_Ret, 0 Sec_Prd_NR_Value, 0 Sec_Prd_NR_Value_Ret,
                sum(Sec_Late_Prd_Qty)Sec_Late_Prd_Qty,sum(Sec_Late_Prd_Qty_Ret)Sec_Late_Prd_Qty_Ret,sum(Sec_Late_Prd_NR_Value)Sec_Late_Prd_NR_Value,sum(Sec_Late_Prd_NR_Value_Ret)Sec_Late_Prd_NR_Value_Ret
                from itg_pf_retail_mth_ds 
                group by case when month=12 then year+1 else year end,case when month=12 then 1 else month+1 end,ifnull(customer_code,'-1'),ifnull(product_code,'-1')
)              
group by year, month, customer_code, product_code
)
select * from final