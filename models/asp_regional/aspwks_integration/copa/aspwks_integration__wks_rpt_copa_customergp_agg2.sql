with wks_rpt_copa_customergp_agg1 as
(
    select * from {{ ref('aspwks_integration__wks_rpt_copa_customergp_agg1') }}
),
wks_rpt_copa_customergp_base as
(
    select * from {{ ref('aspwks_integration__wks_rpt_copa_customergp_base') }}
),
final as
(
    SELECT fisc_yr,
            fisc_yr_per,
            fisc_day,
            ctry_nm,
            "cluster" as cluster,
            sls_org,
            Prft_ctr,
            obj_crncy_co_obj,
            from_crncy,
            to_crncy,
            matl_num,
            cust_num,
            fisc_previousyr_per, 
            COALESCE(ntstt_lcy,0.00) as ntstt_lcy ,
            COALESCE(ntstt_usd,0.00) as ntstt_usd ,
            COALESCE(ntstp_lcy,0.00) as ntstp_lcy ,
            COALESCE(ntstp_usd,0.00) as ntstp_usd ,
            (COALESCE(nts_lcy,0.00) + COALESCE(ntstt_lcy,0.00)+ COALESCE(ntstp_lcy,0.00) ) AS nts_lcy ,
            (COALESCE(nts_usd,0.00) + COALESCE(ntstt_usd,0.00) + COALESCE(ntstp_usd,0.00)) AS nts_usd  ,
            COALESCE(gts_lcy,0.00) as  gts_lcy ,
            COALESCE(gts_usd,0.00) as gts_usd,
            COALESCE(cfreegood_lcy,0.00) as cfreegood_lcy,
            COALESCE(cfreegood_usd,0.00) as cfreegood_usd,
            COALESCE(stdcogs_lcy,0.00) as stdcogs_lcy,
            COALESCE(stdcogs_usd,0.00) as stdcogs_usd,
            COALESCE(rtn_lcy,0.00) as rtn_lcy,
            COALESCE(rtn_usd,0.00) as rtn_usd,     
            COALESCE(glhd_lcy,0.00) as glhd_lcy,
            COALESCE(glhd_usd,0.00) as glhd_usd,
            COALESCE(py_ntstt_lcy,0.00) as py_ntstt_lcy ,
            COALESCE(py_ntstt_usd,0.00) as py_ntstt_usd ,
            COALESCE(py_ntstp_lcy,0.00) as py_ntstp_lcy ,
            COALESCE(py_ntstp_usd,0.00) as py_ntstp_usd ,
            (COALESCE(py_nts_lcy,0.00) + COALESCE(py_ntstt_lcy,0.00)+ COALESCE(py_ntstp_lcy,0.00))  AS py_nts_lcy,
            (COALESCE(py_nts_usd,0.00) + COALESCE(py_ntstt_usd,0.00)+ COALESCE(py_ntstp_usd,0.00))  AS py_nts_usd           ,
            COALESCE(py_gts_lcy,0.00) as py_gts_lcy ,
            COALESCE(py_gts_usd,0.00) as py_gts_usd ,
            COALESCE(py_cfreegood_lcy,0.00) as py_cfreegood_lcy         ,
            COALESCE(py_cfreegood_usd,0.00) as py_cfreegood_usd          ,
            COALESCE(py_stdcogs_lcy,0.00) as   py_stdcogs_lcy          ,
            COALESCE(py_stdcogs_usd,0.00) as  py_stdcogs_usd          ,
            COALESCE(py_rtn_lcy,0.00) as  py_rtn_lcy,
            COALESCE(py_rtn_usd,0.00) as  py_rtn_usd, 
            COALESCE(py_glhd_lcy,0.00) as   py_glhd_lcy ,
            COALESCE(py_glhd_usd,0.00) as   py_glhd_usd                             
    FROM
    (
        select custgp.fisc_yr_dt as fisc_yr,
               custgp.fisc_yr_per_dt as fisc_yr_per,
               custgp.fisc_day_dt as fisc_day,
               custgp.ctry_nm,
               custgp."cluster",
               custgp.sls_org,
               custgp.Prft_ctr,
               custgp.obj_crncy_co_obj,
               custgp.from_crncy,
               custgp.to_crncy,
               custgp.matl_num,
               custgp.cust_num,
               custgp.fisc_previousyr_per_dt as fisc_previousyr_per, 
               cyamt.ntstt_lcy,
               cyamt.ntstt_usd,
               cyamt.ntstp_lcy,
               cyamt.ntstp_usd,
               cyamt.nts_lcy,
               cyamt.nts_usd,
               cyamt.gts_lcy,
               cyamt.gts_usd,
               cyamt.stdcogs_lcy,
               cyamt.stdcogs_usd,
               cyamt.cfreegood_lcy,
               cyamt.cfreegood_usd,
               cyamt.rtn_lcy,
               cyamt.rtn_usd,
               cyamt.glhd_lcy,
               cyamt.glhd_usd,    
               case when custgp.to_crncy != 'USD' then pyamt.ntstt_lcy else 0 end  as py_ntstt_lcy,
               case when custgp.to_crncy = 'USD' then pyamt.ntstt_usd else 0 end as py_ntstt_usd,
               case when custgp.to_crncy != 'USD' then pyamt.ntstp_lcy else 0 end as py_ntstp_lcy,
               case when custgp.to_crncy = 'USD' then pyamt.ntstp_usd else 0 end AS py_ntstp_usd,
               case when custgp.to_crncy != 'USD' then pyamt.nts_lcy else 0 end AS py_nts_lcy,
               case when custgp.to_crncy = 'USD' then pyamt.nts_usd else 0 end AS py_nts_usd,
               case when custgp.to_crncy != 'USD' then pyamt.gts_lcy else 0 end AS py_gts_lcy,
               case when custgp.to_crncy = 'USD' then pyamt.gts_usd else 0 end AS py_gts_usd,
               case when custgp.to_crncy != 'USD' then pyamt.stdcogs_lcy else 0 end AS py_stdcogs_lcy,
               case when custgp.to_crncy = 'USD' then pyamt.stdcogs_usd else 0 end AS py_stdcogs_usd,
               case when custgp.to_crncy != 'USD' then pyamt.cfreegood_lcy else 0 end AS py_cfreegood_lcy,
               case when custgp.to_crncy = 'USD' then pyamt.cfreegood_usd else 0 end AS py_cfreegood_usd,
               case when custgp.to_crncy != 'USD' then pyamt.rtn_lcy else 0 end AS py_rtn_lcy,
               case when custgp.to_crncy = 'USD' then pyamt.rtn_usd else 0 end AS py_rtn_usd,
               case when custgp.to_crncy != 'USD' then pyamt.glhd_lcy else 0 end AS py_glhd_lcy,
               case when custgp.to_crncy = 'USD' then pyamt.glhd_usd else 0 end AS py_glhd_usd       
               from wks_rpt_copa_customergp_agg1 custgp   
               left join (select fisc_yr,
                                fisc_yr_per,
                                ctry_nm,
                                "cluster",
                                sls_org,
                                obj_crncy_co_obj,
                                from_crncy,
                                to_crncy,
                                prft_ctr,
                                cust_num,
                                matl_num,
                                sum(ntstt_lcy) as ntstt_lcy,
                                sum(ntstt_usd) as ntstt_usd,
                                sum(ntstp_lcy) as ntstp_lcy,
                                sum(ntstp_usd) as ntstp_usd,
                                sum(nts_lcy) as nts_lcy,
                                sum(nts_usd) as nts_usd,
                                sum(gts_lcy) as gts_lcy,
                                sum(gts_usd) as gts_usd,
                                sum(rtn_lcy) as rtn_lcy,
                                sum(rtn_usd) as rtn_usd,							
                                sum(stdcogs_lcy) as stdcogs_lcy,
                                sum(stdcogs_usd) as stdcogs_usd,
                                sum(cfreegood_lcy) as cfreegood_lcy,
                                sum(cfreegood_usd) as cfreegood_usd,
                                sum(glhd_lcy) as glhd_lcy,
                                sum(glhd_usd) as glhd_usd		
                                from
                                wks_rpt_copa_customergp_base 
                                group by 1,2,3,4,5,6,7,8,9,10,11) pyamt
                                ON  custgp.fisc_previousyr_per_dt = pyamt.fisc_yr_per
                                AND  coalesce(nullif(custgp.matl_num,''),'0') = coalesce(nullif(pyamt.matl_num,''),'0') 
                                AND  coalesce(nullif(custgp.cust_num,''),'0') = coalesce(nullif(pyamt.cust_num,''),'0')
                                AND  custgp.ctry_nm = pyamt.ctry_nm 
                                AND  coalesce(nullif(custgp.sls_org,''),'0') = coalesce(nullif(pyamt.sls_org,''),'0') 
                                AND  custgp.Prft_ctr = pyamt.Prft_Ctr 
                                AND custgp.to_crncy = pyamt.to_crncy	
            left join (select fisc_yr,
                                fisc_yr_per,
                                ctry_nm,
                                "cluster",
                                sls_org,
                                obj_crncy_co_obj,
                                from_crncy,
                                to_crncy,
                                prft_ctr,
                                cust_num,
                                matl_num,
                                sum(ntstt_lcy) as ntstt_lcy,
                                sum(ntstt_usd) as ntstt_usd,
                                sum(ntstp_lcy) as ntstp_lcy,
                                sum(ntstp_usd) as ntstp_usd,
                                sum(nts_lcy) as nts_lcy,
                                sum(nts_usd) as nts_usd,
                                sum(gts_lcy) as gts_lcy,
                                sum(gts_usd) as gts_usd,
                                sum(rtn_lcy) as rtn_lcy,
                                sum(rtn_usd) as rtn_usd,							
                                sum(stdcogs_lcy) as stdcogs_lcy,
                                sum(stdcogs_usd) as stdcogs_usd,
                                sum(cfreegood_lcy) as cfreegood_lcy,
                                sum(cfreegood_usd) as cfreegood_usd,
                                sum(glhd_lcy) as glhd_lcy,
                                sum(glhd_usd) as glhd_usd		
                                from 							
                                wks_rpt_copa_customergp_base 
                                group by 1,2,3,4,5,6,7,8,9,10,11) cyamt 
                                ON  custgp.fisc_yr_dt = cyamt.fisc_yr
                                    AND custgp.fisc_yr_per_dt = cyamt.fisc_yr_per
                                    AND  coalesce(nullif(custgp.matl_num,''),'0') = coalesce(nullif(cyamt.matl_num,''),'0') 
                                    AND  coalesce(nullif(custgp.cust_num,''),'0') = coalesce(nullif(cyamt.cust_num,''),'0')
                                    AND  custgp.ctry_nm = cyamt.ctry_nm 
                                    AND  coalesce(nullif(custgp.sls_org,''),'0') = coalesce(nullif(cyamt.sls_org,''),'0') 
                                    AND  custgp.Prft_ctr = cyamt.Prft_Ctr 
                                    AND custgp.to_crncy = cyamt.to_crncy                      
    )
)
select * from final