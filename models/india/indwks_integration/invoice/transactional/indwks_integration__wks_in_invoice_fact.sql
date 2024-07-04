{{
    config(
        pre_hook = "{{build_edw_in_invoice_fact_temp()}}"
    )
}}
with 
edw_invoice_fact as 
(
    select * from {{ ref('aspedw_integration__edw_invoice_fact') }}
),
itg_billing_conditions as 
(
    select * from snapinditg_integration.itg_billing_conditions
), --Job status unknown
itg_product_uom_master as 
(
    select * from snapinditg_integration.itg_product_uom_master
), -- job status unknown
itg_businesscalender as 
(
    select * from snapinditg_integration.itg_businesscalender
),
edw_product_dim as 
(
    select * from {{ ref('indedw_integration__edw_product_dim') }}
),
edw_in_invoice_fact_temp as 
(
    select * from {{ source('indedw_integration','edw_in_invoice_fact_temp') }}
),
a as 
(
    select distinct 
    sold_to_prty, 
    matl_num, 
    bill_doc, 
    bill_typ, 
    bill_dt, 
    sls_unit, 
    base_uom from 
    edw_invoice_fact where co_cd='8080' 
),
b as 
( 
    select sold_to, 
    material, 
    bill_num, 
    bill_type, 
    bill_date, 
    sales_unit, 
    sum(knval) as knval,
    sum(ifnull(case when right(inv_qty, 1) = '-' then cast(rtrim(rtrim(inv_qty, '-'),' ') as float)*(-1)
    else cast(inv_qty as float) end,0)) as sls_inv_qty from itg_billing_conditions 
    where knart = 'ZDN2' and comp_code = '8080'  
    group by sold_to, material, bill_num, bill_type, bill_date, sales_unit
),
marm as 
(
    select 
        material, 
        mat_unit, 
        ifnull(numerator,0.00) as numerator 
    from itg_product_uom_master 
        where material is not null 
        and mat_unit is not null 
        and trim(material)<>'' 
        and trim(mat_unit)<>''
),
z as 
(
    select 
        ltrim(a.sold_to_prty,0) as customer_code, 
        ltrim(a.matl_num,0) as product_code, 
        ltrim(a.bill_doc,0) as invoice_no, 
        a.bill_typ as bill_type, 
        a.bill_dt as invoice_date,
        b.knval as nr_value,
        b.sls_inv_qty,
        a.sls_unit as sales_unit,a.base_uom, marm.numerator 
        from a
        left join b
        on ltrim(a.sold_to_prty,0)=b.sold_to
        and ltrim(a.matl_num,0)=b.material  
        and ltrim(a.bill_doc,0)=b.bill_num 
        and a.bill_typ=b.bill_type 
        and a.bill_dt=b.bill_date 
        and a.sls_unit=b.sales_unit  
        left join marm
        on ltrim(a.matl_num,0)=marm.material
        and a.sls_unit=marm.mat_unit
        left join itg_businesscalender cl
        on a.bill_dt=cl.salinvdate   
),
y as 
(   select 
        z.customer_code, 
        z.product_code, 
        z.invoice_no, 
        z.bill_type, 
        z.invoice_date,
        z.nr_value, 
        z.sls_inv_qty, 
        z.sales_unit, 
        p.net_wt,
        case when z.sales_unit=z.base_uom then z.sls_inv_qty
            else z.sls_inv_qty*z.numerator end as inv_qty
    from z
        left join edw_product_dim p on z.product_code=p.product_code 
),
x as 
(
    select 
        y.customer_code, 
        y.product_code, 
        y.invoice_no, 
        y.invoice_date, 
        sum(y.inv_qty) as inv_qty,
        sum(case when upper(y.bill_type) = 'ZF2D' then y.nr_value else 0 end) as m,
        sum(case when upper(y.bill_type) = 'ZG2D' then y.nr_value else 0 end) as n, 
        sum(case when upper(y.bill_type) = 'ZC2D' then y.nr_value else 0 end) as o,
        sum(case when upper(y.bill_type) = 'ZL2D' then y.nr_value else 0 end) as p,
        sum(case when upper(y.bill_type) = 'ZG22' then y.nr_value else 0 end) as q, 
        sum(case when upper(y.bill_type) = 'ZC22' then y.nr_value else 0 end) as r,
        sum(case when upper(y.bill_type) = 'ZL22' then y.nr_value else 0 end) as s,
        sum(case when upper(y.bill_type) = 'ZF2D' then y.inv_qty  else 0 end) as t,
        sum(case when upper(y.bill_type) = 'ZG2D' then y.inv_qty  else 0 end) as u,
        sum(case when upper(y.bill_type) = 'ZG22' then y.inv_qty  else 0 end) as v,
        sum(y.inv_qty*y.net_wt)/1000 as wt_invoice_qty 
    from y
    group by 
        y.customer_code, 
        y.product_code, 
        y.invoice_no, 
        y.invoice_date
),
src as 
(
    select 
        x.customer_code, 
        x.product_code, 
        x.invoice_no, 
        x.invoice_date,
        m-n-o+p-q-r+s as invoice_val, 
        t-u-v as invoice_qty,
        wt_invoice_qty,
        current_timestamp() as crt_dttm, 
        current_timestamp() as updt_dttm
    from x
),
tgt as 
(
        select 
            customer_code,
            product_code, 
            invoice_no, 
            invoice_date,
            invoice_val,
            invoice_qty, 
            wt_invoice_qty,
            crt_dttm 
        from edw_in_invoice_fact_temp 
),
trans as 
(
    select 
        src.customer_code,
        src.product_code, 
        src.invoice_no, 
        src.invoice_date,
        src.invoice_val,
        src.invoice_qty, 
        src.wt_invoice_qty,
        case  when tgt.crt_dttm is null then current_timestamp()
                else tgt.crt_dttm 
                end as crt_dttm ,
            case when tgt.crt_dttm is null then null 
                else current_timestamp() 
                end as updt_dttm,
            case when tgt.crt_dttm is null then 'I' 
                else 'U' 
                end as chng_flg 
        from src
        left outer join tgt
            on 	src.customer_code = tgt.customer_code 
            and src.product_code = tgt.product_code 
            and src.invoice_no = tgt.invoice_no 
            and nvl(src.invoice_date,'9999-01-01') = nvl(tgt.invoice_date,'9999-01-01')
            and src.invoice_val = tgt.invoice_val
            and src.invoice_qty = tgt.invoice_qty
),
final as 
(
    select 
    customer_code::varchar(10) as customer_code,
	product_code::varchar(18) as product_code,
	invoice_no::varchar(10) as invoice_no,
	invoice_date::date as invoice_date,
	invoice_val::number(38,17) as invoice_val,
	invoice_qty::float as invoice_qty,
	wt_invoice_qty::float as wt_invoice_qty,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm,
	chng_flg::varchar(1) as chng_flg
    from trans
)
select * from final