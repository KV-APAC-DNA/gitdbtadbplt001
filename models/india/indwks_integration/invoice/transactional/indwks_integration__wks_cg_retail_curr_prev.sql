with 
itg_pf_retail_mth_ds as 
(
    select * from {{ ref('inditg_integration__itg_pf_retail_mth_ds') }}
),
trans  as 
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
),
final as 
(
    select 
	year::number(18,0) as year,
	month::number(18,0) as month,
	customer_code::varchar(50) as customer_code,
	product_code::varchar(50) as product_code,
	sec_prd_qty::number(38,4) as sec_prd_qty,
	sec_prd_qty_ret::number(38,4) as sec_prd_qty_ret,
	sec_prd_nr_value::number(38,3) as sec_prd_nr_value,
	sec_prd_nr_value_ret::number(38,3) as sec_prd_nr_value_ret,
	sec_late_prd_qty::number(38,4) as sec_late_prd_qty,
	sec_late_prd_qty_ret::number(38,4) as sec_late_prd_qty_ret,
	sec_late_prd_nr_value::number(38,3) as sec_late_prd_nr_value,
	sec_late_prd_nr_value_ret::number(38,3) as sec_late_prd_nr_value_ret
    from trans
)
select * from final