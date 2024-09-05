with 
wks_otc_inv_si as 
(
    select * from asing012_workspace.wks_otc_inv_si
),
edw_material_sales_dim as 
(
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
edw_calendar_dim as 
(
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
edw_billing_fact as 
(
    select * from {{ ref('aspedw_integration__edw_billing_fact') }}
),
BILL_FACT as 
      (SELECT material,
                       cust_sls,
                       bill_dt,
                       bill_qty,
                       netval_inv,
                       cast(netval_inv / bill_qty as numeric(15,4) ) InvoicePrice,
                       ROW_NUMBER() OVER (PARTITION BY material,cust_sls ORDER BY bill_dt DESC) rn
                FROM edw_billing_fact bill
                WHERE sls_org in ('3200','320A','320S','321A') 
                AND   bill_type = 'ZF2K'
    	),

 previous_sellout AS
(
  SELECT  data_type,
         dstr_cd,
         prod_cd,
         fisc_per AS curr_mnth,
         MONTH as prev_mnth,
         inv_qty,
         inv_value
FROM (SELECT t1.data_type,
               t1.dstr_cd,
               t1.prod_cd,
               t2.fisc_per,
               MONTH,
               t1.inv_qty,
               (inv_qty * NVL(t5.amount,t6.amount)) AS inv_value
       FROM wks_otc_inv_si t1,
             (SELECT fisc_yr,
                     (SUBSTRING(fisc_per,1,4) ||substring (fisc_per,6,2)) as fisc_per,
                     TO_CHAR(dateadd ('month',-1,TO_DATE((SUBSTRING(fisc_per,1,4) ||substring (fisc_per,6,2)),'YYYYMM')),'YYYYMM') AS MONTH
             FROM edw_calendar_dim group by 1,2,3) t2,
             (SELECT distinct  LTRIM(material,0) material,
    				cust_sls,
    				(cast(InvoicePrice as numeric(10,4))) AS amount
    				FROM BILL_FACT 
    				WHERE rn = 1) T5 , 		
    		 (Select distinct t_msd.matl_num,
    						t_ean.*
    				  from 
    					(Select cust_sls,ean_num,(cast(price as numeric(10,4)))as amount 
    					 from 
    						(	
    						Select ivp.*,
    							  (row_number() over (partition by ivp.cust_sls  order by ivp.price,ivp.ean_num desc ))rn 
    						from ( Select distinct 
    							msd.matl_num,
    							msd.ean_num,
    							bill.*
    							from 
    							edw_material_sales_dim msd,
    							(SELECT distinct  LTRIM(material,0) material,
    									cust_sls,
    									InvoicePrice AS price
    							 FROM BILL_FACT 
    									WHERE rn = 1) bill
    							WHERE msd.sls_org in ('3200','320A','320S','321A') 
    							AND  LTRIM(msd.matl_num,'0') = LTRIM(bill.material,'0')
    							and ean_num!='')ivp
    						)t_ivp 
    					where rn=1
    					--group by ean_num
    					) t_ean,
    					edw_material_sales_dim t_msd
    					where t_msd.ean_num=t_ean.ean_num
    					and t_msd.sls_org in ('3200','320A','320S','321A')
    		) T6
        WHERE --LEFT (t1.fisc_per,4) = t2.fisc_yr
           t1.fisc_per = t2.month
        AND   t1.data_type = 'INV' --and fisc_per='202003'
        AND  ltrim(T1.dstr_cd,0)=ltrim(t5.cust_sls(+),0) 
AND   Ltrim(T1.prod_cd,0)=ltrim(T5.material(+),0)
AND   Ltrim(T1.prod_cd,0)=ltrim(T6.matl_num(+),0)
AND  ltrim(T1.dstr_cd,0)=ltrim(T6.cust_sls(+),0))) --group by 1,2,3,4,5


SELECT dstr_cd,
       prod_cd,
       fisc_per as month,
       SUM(si_qty) AS sellin_qty,
       SUM(si_value) AS sellin_val,
       SUM(inv_qty) AS inv_qty,
       SUM(inv_value) AS inv_value,
       nvl(SUM(nvl(prv_mnth_inv_qty,0)) + sum(nvl(si_qty,0)) - sum(nvl(inv_qty,0)),0) AS so_qty,
       nvl(SUM(nvl(prv_mnth_inv_value,0)) + sum(nvl(si_value,0)) - sum(nvl(inv_value,0)),0) AS so_value
FROM (SELECT curr.dstr_cd,
             curr.prod_cd,
             curr.fisc_per,
             SUM(si_qty) AS si_qty,
             SUM(si_value) AS si_value,
             SUM(inv_qty) AS inv_qty,
             sum(inv_value) as inv_value,
             SUM(prv_mnth_inv_qty) AS prv_mnth_inv_qty,
             SUM(prv_mnth_inv_value) AS prv_mnth_inv_value
      FROM (SELECT dstr_cd,
                   prod_cd,
                   fisc_per,
                   SUM(si_qty) AS si_qty,
                   SUM(si_value) AS si_value,
                   SUM(inv_qty) AS inv_qty,
                   sum(inv_qty * NVL(t5.amount,t6.amount)) as inv_value
                   FROM wks_otc_inv_si as t1,
                                (SELECT distinct  LTRIM(material,0) material,
    				cust_sls,
    				(cast(InvoicePrice as numeric(10,4))) AS amount
    				FROM BILL_FACT 
    				WHERE rn = 1) T5 , 		
    		 (Select distinct t_msd.matl_num,
    						t_ean.*
    				  from 
    					(Select cust_sls,ean_num,(cast(price as numeric(10,4)))as amount 
    					 from 
    						(	
    						Select ivp.*,
    							  (row_number() over (partition by ivp.cust_sls  order by ivp.price,ivp.ean_num desc ))rn 
    						from ( Select distinct 
    							msd.matl_num,
    							msd.ean_num,
    							bill.*
    							from 
    							edw_material_sales_dim msd,
    							(SELECT distinct  LTRIM(material,0) material,
    									cust_sls,
    									InvoicePrice AS price
    							 FROM BILL_FACT 
    									WHERE rn = 1) bill
    							WHERE msd.sls_org in ('3200','320A','320S','321A') 
    							AND  LTRIM(msd.matl_num,'0') = LTRIM(bill.material,'0')
    							and ean_num!='')ivp
    						)t_ivp 
    					where rn=1
    					) t_ean,
    					edw_material_sales_dim t_msd
    					where t_msd.ean_num=t_ean.ean_num
    					and t_msd.sls_org in ('3200','320A','320S','321A')
    		) T6
    		where   ltrim(T1.dstr_cd,0)=ltrim(t5.cust_sls(+),0) 
AND   Ltrim(T1.prod_cd,0)=ltrim(T5.material(+),0)
AND   Ltrim(T1.prod_cd,0)=ltrim(T6.matl_num(+),0)
AND  ltrim(T1.dstr_cd,0)=ltrim(T6.cust_sls(+),0)
            GROUP BY 1,
                     2,
                     3) curr,
           (SELECT dstr_cd,
                   prod_cd,
                   curr_mnth,
                   SUM(inv_qty) AS prv_mnth_inv_qty,
                   sum(inv_value) as prv_mnth_inv_value
            FROM previous_sellout            
            GROUP BY 1,
                     2,
                     3) AS prev
      WHERE curr.dstr_cd = prev.dstr_cd(+) 
      AND   curr.prod_cd = prev.prod_cd(+)
      AND   fisc_per = curr_mnth(+) 
      group by 1,2,3)
GROUP BY 1,2,3