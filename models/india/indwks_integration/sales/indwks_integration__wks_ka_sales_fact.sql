{{
    config(
        pre_hook = "{{build_edw_ka_sales_fact_temp()}}"
    )
}}
with itg_keyaccountsales as
(
    select * from {{ ref('inditg_integration__itg_keyaccountsales') }}
),
edw_ka_sales_fact as 
(
    select * from {{ source('indedw_integration', 'edw_ka_sales_fact_temp') }}
),
final as
(
SELECT src.customer_code,
	src.invoice_date,
	src.retailer_code,
	src.retailer_name,
	src.product_code,
	src.invoice_no,
	prdqty,
	PrdTaxamt,
	prdschdiscamt,
	prddbdiscamt,
	salwdsamt,
	src.saleflag,
	src.confirmsales,
	subtotal4,
	totalgrosssalesincltax,
	totalsalesnr,
	totalsalesconfirmed,
	totalsalesnrconfirmed,
	totalsalesunconfirmed,
	totalsalesnrunconfirmed,
	totalqtyconfirmed,
	totalqtyunconfirmed,
	src.buyingoutlets,
	CASE 
	WHEN tgt.crt_dttm IS NULL THEN current_timestamp()
	ELSE tgt.crt_dttm 
	END AS crt_dttm ,
	CASE 
	WHEN tgt.crt_dttm IS NULL THEN NULL 
	ELSE current_timestamp() 
	END AS updt_dttm,
	CASE 
	WHEN tgt.crt_dttm IS NULL THEN 'I' 
	ELSE 'U' 
	END AS chng_flg 
	FROM 
		(Select 
		Distributorcode customer_code,
		CAST(SUBSTRING(SalInvDate,1,4)||SUBSTRING(SalInvDate,6,2)||SUBSTRING(SalInvDate,9,2) as Integer)  Invoice_Date,
		Rtrcode Retailer_Code,
		Rtrnm Retailer_Name,
		ltrim(Prdccode,0) as Product_Code,  ---- added ltrim
		salinvno Invoice_No,
		Sum(PrdQty) PrdQty,
		Sum(PrdTaxamt) PrdTaxamt,
		Sum(Prdschdiscamt) Prdschdiscamt,
		Sum(Prddbdiscamt) Prddbdiscamt,
		Sum(Salwdsamt) Salwdsamt,
		SaleFlag,
		ConfirmSales,
		Sum(SubTotal4) SubTotal4,
		Sum((prdqty*prdsalerate)+prdtaxamt) TotalGrossSalesInclTax,
		Sum(prdqty*netrate) TotalSalesNR,
		Sum(case when confirmsales='Y' then (prdqty*prdsalerate)+prdtaxamt else 0 end) TotalSalesConfirmed,
		Sum(case when confirmsales='Y' 
		then 
		case 
		when Saleflag in ('WIS', 'WIR') then Netrate   ------- for winculum we have taken only Netrate
		when Subtotal4 IS NULL then PrdQty*Netrate
		when Saleflag='DS' then Subtotal4
		else PrdQty*Netrate end
		else 0 end) TotalSalesNRConfirmed,  
		Sum(case when confirmsales='N' then (prdqty*prdsalerate)+prdtaxamt else 0 end) TotalSalesUnConfirmed,
		Sum(case when confirmsales='N' 
		then 
		case when Subtotal4 IS NULL then PrdQty*Netrate
		when Saleflag='DS' then Subtotal4
		else PrdQty*Netrate end
		else 0 end) TotalSalesNRUnConfirmed,
		Sum(case when ConfirmSales='Y' then PrdQty else 0 end) TotalQtyConfirmed,
		Sum(case when ConfirmSales='N' then PrdQty else 0 end) TotalQtyUnConfirmed,
		Distributorcode||'_'||(case when Rtrcode is null or trim(RtrCode) = '' then 'NA' else Rtrcode end) BuyingOutlets
		From itg_keyaccountsales
		Group By Distributorcode, SalInvDate, Rtrcode, Rtrnm, Prdccode, salinvno, SaleFlag, ConfirmSales,
		Distributorcode||'_'||(case when Rtrcode is null or trim(RtrCode) = '' then 'NA' else Rtrcode end) 
		)src
	LEFT OUTER JOIN 
		(Select 
		customer_code, invoice_date, retailer_code, retailer_name, ltrim(product_code, 0) as product_code, invoice_no, saleflag, ConfirmSales, BuyingOutlets, crt_dttm
		From edw_ka_sales_fact) tgt
	ON src.customer_code = tgt.customer_code 
	AND src.invoice_date = tgt.invoice_date
	AND src.retailer_code = tgt.retailer_code
	AND src.retailer_name = tgt.retailer_name
	AND ltrim(src.product_code,0) = ltrim(tgt.product_code,0)
	AND src.invoice_no = tgt.invoice_no
	AND src.saleflag = tgt.saleflag
	AND src.ConfirmSales = tgt.ConfirmSales
	AND src.BuyingOutlets = tgt.BuyingOutlets
)
select * from final
