with vw_edw_kr_ecom_dstr_inventory  as 
(
    select * from {{ ref('ntaedw_integration__vw_edw_kr_ecom_dstr_inventory') }}
),
vw_edw_kr_ecom_dstr_sellout as 
(
    select * from {{ ref('ntaedw_integration__vw_edw_kr_ecom_dstr_sellout') }}
),
edw_billing_fact  as 
(
    select * from {{ ref('aspedw_integration__edw_billing_fact') }}
),
edw_vw_os_time_dim as 
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_billing_fact as 
(
    select * from {{ ref('aspedw_integration__edw_billing_fact') }}
),
edw_material_sales_dim as 
(
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
edw_otc_inv_si_so as 
(
    select * from {{ ref('ntaedw_integration__edw_otc_inv_si_so') }}
),
final as (
Select 
dstr_cd,prod_cd,fisc_per,
sum(si_sls_qty) as si_qty,
sum(si_gts_val) as si_Val,
sum(inventory_quantity) as inv_qty,
sum(inventory_val) as inv_Value,
sum(so_sls_qty) as so_qty,
sum(so_trd_sls) as so_value
from 
(with SISO as 
      (
     SELECT 'Ecomm' as subj_area,
   		           'KR' ctry_cd,
                  'INV' Data_Type,
                  inv.dstr_cd as dstr_cd,
  				        inv.matl_num as prod_cd,
                   --inv.ean,
                  substring(inv.inv_date,1,4)||substring(inv.inv_date,6,2) as fisc_per,
                   SUM(inventory_qty) inv_qty,
                   0 as  inv_value,
                   0 AS so_qty,
                   0 AS so_value,
                   0 AS si_qty,
                   0 AS si_value
          FROM vw_edw_kr_ecom_dstr_inventory inv
             JOIN (SELECT dstr_cd, 
                          substring(inv.inv_date,1,4)||substring(inv.inv_date,6,2) as fisc_per, --fisc_per,
              			      matl_num,
              			      ean,
                          MAX(inv_date) max_invnt_dt
                   FROM vw_edw_kr_ecom_dstr_inventory inv
                                     GROUP BY dstr_cd,
                             substring(inv.inv_date,1,4)||substring(inv.inv_date,6,2),
              				       matl_num,
              				       ean 
  				      ) inv_max
                ON inv.dstr_cd  = inv_max.dstr_cd 
               AND inv.matl_num = inv_max.matl_num  
  			       AND inv.ean  	  = inv_max.ean 
               AND inv.inv_date = inv_max.max_invnt_dt                           
          WHERE left(inv.inv_date,4) >=(date_part(year, convert_timezone('UTC', current_timestamp())) -6)
               --and inv.dstr_cd = '135124' 
               --and left(inv.inv_date,4) = 2020
          GROUP BY inv.dstr_cd,
                   inv.matl_num,
                   --inv.ean,
                   substring(inv.inv_date,1,4)||substring(inv.inv_date,6,2)
              
        UNION ALL 

    		SELECT 'Ecomm' as subj_area,
    		           'KR' ctry_cd,
                     'SELLOUT' Data_Type,
                     dstr_cd as dstr_cd,
    				         matl_num as prod_cd,
                     --ean,
                     substring(so_date,1,4)||substring(so_date,6,2) as fisc_per,
                     0 as inv_qty ,
                     0 as inv_value,
                     SUM(so_qty) so_qty,
                     0 AS so_value,
                     0 AS si_qty,
                     0 AS si_value
           FROM vw_edw_kr_ecom_dstr_sellout so                        
            WHERE left(so_date,4) >=(date_part(year, convert_timezone('UTC', current_timestamp())) -6)
                       GROUP BY dstr_cd,
                     matl_num,
                     substring(so_date,1,4)||substring(so_date,6,2)  
 					 
        UNION ALL

    	   SELECT 'Ecomm' as subj_area,
    	      cntry_cd 
        		 ,data_type
        		 ,distributor_code
        		 ,product_code
        		 ,mnth_id as fisc_per
        		 ,0 AS inv_qty 
        		 ,0 AS inv_amt 
        		 ,0 AS so_qty 
        		 ,0 AS so_value	 
        		 ,sum(sell_in_qty)si_qty
        		 ,sum(sell_in_value)si_value
        	 FROM  
        		(
                SELECT 'KR' cntry_cd 
                    			  ,'SELLIN' as data_type
                    			  ,LTRIM(sold_to,'0') AS distributor_code
                    			  ,LTRIM(MATERIAL,'0') AS product_code 		
                    			  ,bill_dt
                    			  ,sum(bill_qty) sell_in_qty 
                    			  ,sum(netval_inv) sell_in_value  
                    		 FROM edw_billing_fact 
                    		WHERE left(bill_dt,4) >=(date_part(year, convert_timezone('UTC', current_timestamp())) -6)
                    		  AND sls_org in ('3200','320A','320S','321A') and LTRIM(sold_to,'0') in (select distinct dstr_cd from vw_edw_kr_ecom_dstr_sellout)
                    		  AND BILL_TYPE in ('ZF2K' ,'ZG2K')
                    		 GROUP BY LTRIM(sold_to,'0') 
                    			  ,LTRIM(MATERIAL,'0')
                    			  ,bill_dt 
        		) T1
        		left join EDW_VW_OS_TIME_DIM T2 
        		on  to_date(T1.bill_dt)=to_date(T2.cal_date) 
        	group by cntry_cd,
        			data_type,
        			distributor_code,
        			product_code,
        			mnth_id
        			---------------------- OTC product -------------------
        			UNION ALL
        			SELECT 'OTC' as subj_area,
        			      'KR' ctry_cd,
                    null as Data_Type,
                  inv.dstr_cd as dstr_cd,
  				        inv.prod_cd as prod_cd,
                   --inv.ean,
                  inv.month as fisc_per,
                   SUM(inv_qty) inv_qty,
                   0 as  inv_value,
                   SUM(so_qty) so_qty,
                   sum(so_value) as so_value ,
                   sum(Sellin_qty) AS si_qty,
                   sum(sellin_val) AS si_value
          FROM edw_otc_inv_si_so inv                         
          WHERE left(inv.month,4) >=(date_part(year, convert_timezone('UTC', current_timestamp())) -6)
               --and inv.dstr_cd = '135298' 
               --and left(inv.inv_date,4) = 2020
          GROUP BY inv.dstr_cd,
                   inv.prod_cd,
                   --inv.ean,
                   inv.month
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
    	)
Select subj_area,dstr_cd,prod_cd,fisc_per,
        SUM(T1.si_qty) AS SI_SLS_QTY,
        SUM(T1.si_value) AS SI_GTS_VAL,
        SUM(T1.inv_qty) AS INVENTORY_QUANTITY,
        SUM(T1.inv_qty*nvl(t5.amount,t6.amount)) AS INVENTORY_VAL,
        SUM(T1.SO_QTY) AS SO_SLS_QTY,
        CASE WHEN upper(subj_area)='OTC' THEN SUM(T1.so_value)
        ELSE SUM(T1.SO_QTY*nvl(t5.amount,t6.amount)) END AS SO_TRD_SLS

 from 
SISO t1,
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
    		)t6
where     		
      ltrim(T1.dstr_cd,0)=ltrim(t5.cust_sls(+),0) 
AND   Ltrim(T1.prod_cd,0)=ltrim(T5.material(+),0)
AND   Ltrim(T1.prod_cd,0)=ltrim(T6.matl_num(+),0)
AND  ltrim(T1.dstr_cd,0)=ltrim(T6.cust_sls(+),0) 
group by subj_area,dstr_cd,prod_cd,fisc_per
)
group by dstr_cd,prod_cd,fisc_per) select * from final