Drop table if exists rg_wks.wks_edw_perfect_store_kpi_wt_mnth_cname;

-- create table rg_wks.wks_edw_perfect_store_kpi_wt_mnth_cname
-- as
-- select country, nvl(customername,'x') as customername, to_char(scheduleddate,'YYYYMM') scheduledmonth, kpi, min(cast(kpi_chnl_wt as numeric(8,4)) ) chnl_wt 
--   from rg_wks.WKS_EDW_PERFECT_STORE_HASH
--  where 
-- --        nvl(kpi_chnl_wt,0) > 0 
-- --   and  
--       kpi in ('MSL COMPLIANCE','OOS COMPLIANCE')
--    and country in ('Hong Kong','Korea','Taiwan')
-- group by country, customername, to_char(scheduleddate,'YYYYMM'), kpi;


-- create table  rg_wks.wks_edw_perfect_store_kpi_wt_mnth
-- as 
-- Insert into rg_wks.wks_edw_perfect_store_kpi_wt_mnth_cname
-- select country, nvl(customername,'x') as customername, to_char(scheduleddate,'YYYYMM') scheduledmonth, kpi, min(cast(kpi_chnl_wt as numeric(8,4))) chnl_wt 
--   from rg_wks.WKS_EDW_PERFECT_STORE_HASH 
--  where 
-- --        nvl(kpi_chnl_wt,0) > 0  
-- --   and  
--   kpi in ('PROMO COMPLIANCE','PLANOGRAM COMPLIANCE','DISPLAY COMPLIANCE')
--   and  REF_VALUE = 1 
--   and country in ('Hong Kong','Korea','Taiwan')
-- group by country, customername, to_char(scheduleddate,'YYYYMM') , kpi;

-- drop table if exists rg_wks.wks_perfect_store_sos_soa_mnth ;

-- create table rg_wks.wks_perfect_store_sos_soa_mnth as 
-- select * --country, customerid, scheduleddate, kpi, kpi_chnl_wt 
--   from rg_wks.WKS_EDW_PERFECT_STORE_HASH 
--  where  kpi in ('SOS COMPLIANCE','SOA COMPLIANCE') 
--    and  mkt_share  is NOT NULL 
--    and  QUES_TYPE in ('DENOMINATOR','NUMERATOR')
--    and  REGEXP_COUNT(ACTUAL_VALUE, '^[0.00-9]+$') > 0
-- --   and country = 'Taiwan'
--    and (country, customername, scheduleddate, PROD_HIER_L4, PROD_HIER_L5)
--    in 
--    (
--     select country, customername, scheduleddate, PROD_HIER_L4, PROD_HIER_L5 
--       from rg_wks.WKS_EDW_PERFECT_STORE_HASH 
--      where kpi in ('SOS COMPLIANCE','SOA COMPLIANCE') 
--       and  PROD_HIER_L4 is NOT NULL 
--       and  PROD_HIER_L5 is NOT NULL
--       and  mkt_share  is NOT NULL 
--       and  REGEXP_COUNT(ACTUAL_VALUE, '^[0.00-9]+$') > 0
--       and  QUES_TYPE in ('DENOMINATOR','NUMERATOR')
-- --        and country = 'Taiwan'
--      and country  in ('Hong Kong','Korea','Taiwan')
--      group by country, customername, scheduleddate, PROD_HIER_L4, PROD_HIER_L5
--      having count ( distinct QUES_TYPE) = 2
--    );

-- Insert into rg_wks.wks_edw_perfect_store_kpi_wt_mnth_cname
-- -- create table rg_wks.wks_edw_perfect_store_kpi_wt_mnth
-- -- as
-- SELECT country, nvl(customername,'x') as customername, to_char(scheduleddate,'YYYYMM') scheduledmonth, kpi, min(cast(kpi_chnl_wt as numeric(8,4))) chnl_wt 
--   FROM rg_wks.wks_perfect_store_sos_soa_mnth
-- group by country, customername, to_char(scheduleddate,'YYYYMM') , kpi;


-- Drop table if exists rg_wks.wks_edw_perfect_store_kpi_agg_wt_mnth_cname;

-- create table rg_wks.wks_edw_perfect_store_kpi_agg_wt_mnth_cname
-- as
-- select country, customername, scheduledmonth, sum(chnl_wt) total_weight, 
--        case when sum(chnl_wt) = 1 
--              then 1 
--             when sum(chnl_wt) <= 0
--              then 0
--             else 
--                 1/sum(chnl_wt) 
--             end as calc_weight
--  from  rg_wks.wks_edw_perfect_store_kpi_wt_mnth_cname
-- group by country, customername, scheduledmonth;

-- Drop table if exists rg_wks.wks_edw_perfect_store_kpi_rebased_wt_mnth_cname ;

-- create table rg_wks.wks_edw_perfect_store_kpi_rebased_wt_mnth_cname 
-- as 
-- select wt.country, wt.customername,wt.scheduledmonth, wt.kpi, wt.chnl_wt, 
--        agg_wt.total_weight, agg_wt.calc_weight, 
--        case when kpi ='MSL COMPLIANCE' then 
--            chnl_wt * calc_weight end  as weight_msl, 
--        case when kpi = 'OOS COMPLIANCE' then
--            chnl_wt * calc_weight end  as weight_oos, 
--        case when kpi = 'SOA COMPLIANCE' then
--            chnl_wt * calc_weight end  as weight_soa, 
--        case when kpi = 'SOS COMPLIANCE' then 
--            chnl_wt * calc_weight end  as weight_sos, 
--        case when kpi = 'PROMO COMPLIANCE' then 
--            chnl_wt * calc_weight end  as weight_promo, 
--        case when kpi = 'PLANOGRAM COMPLIANCE' then 
--            chnl_wt * calc_weight end  as weight_planogram, 
--        case when kpi = 'DISPLAY COMPLIANCE' then 
--            chnl_wt * calc_weight end  as weight_display 
--  from  rg_wks.wks_edw_perfect_store_kpi_wt_mnth_cname  wt,
--        rg_wks.wks_edw_perfect_store_kpi_agg_wt_mnth_cname agg_wt
--  where  wt.country       = agg_wt.country
--    and  wt.customername    = agg_wt.customername
--    and  wt.scheduledmonth = agg_wt.scheduledmonth  
-- ;
   
  
-- select distinct kpi from rg_edw.edw_perfect_store;
-- 
-- select count(*) from rg_wks.wks_edw_perfect_store_kpi_wt; --308397
-- 
-- select count(*) from  rg_wks.wks_edw_perfect_store_kpi_rebased_wt ; --308397
-- 
-- select * from rg_wks.wks_edw_perfect_store_kpi_rebased_wt limit 100;

drop table if exists rg_edw.EDW_PERFECT_STORE_KPI_DATA; --EDW_PERFECT_STORE_REBASE_WT_V2

create table rg_edw.EDW_PERFECT_STORE_KPI_DATA
as 
select per_str.*
       ,reb_wt.total_weight
       ,calc_weight
       ,weight_msl
       ,weight_oos
       ,weight_soa
       ,weight_sos
       ,weight_promo
       ,weight_planogram
       ,weight_display 
 from ( select * 
          from rg_wks.WKS_EDW_PERFECT_STORE_HASH
          where  kpi in ('MSL COMPLIANCE','OOS COMPLIANCE')
            --and nvl(kpi_chnl_wt,0) > 0 
            and country  in ('Hong Kong','Korea','Taiwan')
        ) per_str
      ,rg_wks.wks_edw_perfect_store_kpi_rebased_wt_mnth_cname  reb_wt
where per_str.country       = reb_wt.country      
  and per_str.customername    = reb_wt.customername   
  and to_char(per_str.scheduleddate,'YYYYMM') = reb_wt.scheduledmonth 
  and per_str.kpi           = reb_wt.kpi 
  
 -- and per_str.customerid = 'f0dfe236-d8e9-4af9-8146-a7e00de3541d-MySales' 
 -- and per_str.scheduleddate = '2019-05-28'

;

-- Insert into rg_edw.edw_perfect_store_rebase_wt_mnth 
-- select per_st.*
--   from rg_wks.wks_edw_perfect_store_hash per_st
--           where  kpi in ('MSL COMPLIANCE','OOS COMPLIANCE')
--             -- and country = 'Korea'
-- minus
-- select per_str.*
--   from rg_wks.wks_edw_perfect_store_hash per_str
--           where  kpi in ('MSL COMPLIANCE','OOS COMPLIANCE')
--             --and  nvl(kpi_chnl_wt,0) > 0 
--             --and country = 'Korea'
--             ;
            
-- create table rg_edw.edw_perfect_store_rebase_wt_mnth
-- as 
Insert into rg_edw.EDW_PERFECT_STORE_KPI_DATA 
select per_str.*
       ,reb_wt.total_weight
       ,calc_weight
       ,weight_msl
       ,weight_oos
       ,weight_soa
       ,weight_sos
       ,weight_promo
       ,weight_planogram
       ,weight_display 
 from  (select * 
          from rg_wks.WKS_EDW_PERFECT_STORE_HASH
         where kpi in ('PROMO COMPLIANCE','PLANOGRAM COMPLIANCE','DISPLAY COMPLIANCE') 
           and REF_VALUE = 1 
           --and nvl(kpi_chnl_wt,0) > 0  
           and country in ('Hong Kong','Korea','Taiwan')
        )per_str
      ,rg_wks.wks_edw_perfect_store_kpi_rebased_wt_mnth_cname  reb_wt
where per_str.country       = reb_wt.country     
  and per_str.customername  = reb_wt.customername  
  and to_char(per_str.scheduleddate,'YYYYMM') = reb_wt.scheduledmonth 
  and per_str.kpi           = reb_wt.kpi        
 -- and per_str.customerid = 'f0dfe236-d8e9-4af9-8146-a7e00de3541d-MySales' 
 -- and per_str.scheduleddate = '2019-05-28'
;

   
Insert into rg_edw.EDW_PERFECT_STORE_KPI_DATA 
select per_st.*
  from rg_wks.WKS_EDW_PERFECT_STORE_HASH per_st
          where  kpi in ('PROMO COMPLIANCE','PLANOGRAM COMPLIANCE','DISPLAY COMPLIANCE') 
             and country in ('Hong Kong','Korea','Taiwan')
minus
select per_str.*
  from rg_wks.WKS_EDW_PERFECT_STORE_HASH per_str
          where  kpi in ('PROMO COMPLIANCE','PLANOGRAM COMPLIANCE','DISPLAY COMPLIANCE') 
           and REF_VALUE = 1 
           --and nvl(kpi_chnl_wt,0) > 0  
            and country in ('Hong Kong','Korea','Taiwan')
            ;
-- create table rg_edw.edw_perfect_store_rebase_wt_mnth 
-- as   
Insert into rg_edw.EDW_PERFECT_STORE_KPI_DATA
select per_str.*
       ,reb_wt.total_weight
       ,calc_weight
       ,weight_msl
       ,weight_oos
       ,weight_soa
       ,weight_sos
       ,weight_promo
       ,weight_planogram
       ,weight_display 
 from  rg_wks.wks_perfect_store_sos_soa_mnth per_str
      ,rg_wks.wks_edw_perfect_store_kpi_rebased_wt_mnth_cname  reb_wt
where per_str.country       = reb_wt.country 
  and per_str.customername  = reb_wt.customername 
  and to_char(per_str.scheduleddate,'YYYYMM') = reb_wt.scheduledmonth 
  and per_str.kpi           = reb_wt.kpi
 -- and per_str.customerid = 'f0dfe236-d8e9-4af9-8146-a7e00de3541d-MySales' 
 -- and per_str.scheduleddate = '2019-05-28'
 ;

Insert into rg_edw.EDW_PERFECT_STORE_KPI_DATA 
select * --country, customerid, scheduleddate, kpi, kpi_chnl_wt 
  from rg_wks.WKS_EDW_PERFECT_STORE_HASH 
 where kpi in ('SOS COMPLIANCE','SOA COMPLIANCE') 
 and country  in ('Hong Kong','Korea','Taiwan')
minus
select * 
  from rg_wks.wks_perfect_store_sos_soa_mnth; 
  
TRUNCATE TABLE RG_EDW.EDW_PERFECT_STORE_REBASE_WT;

INSERT INTO RG_EDW.EDW_PERFECT_STORE_REBASE_WT
(
  hashkey,
  hash_row,
  dataset,
  customerid,
  salespersonid,
  visitid,
  questiontext,
  productid,
  kpi,
  scheduleddate,
  latestdate,
  fisc_yr,
  fisc_per,
  merchandiser_name,
  customername,
  country,
  state,
  parent_customer,
  retail_environment,
  channel,
  retailer,
  business_unit,
  eannumber,
  matl_num,
  prod_hier_l1,
  prod_hier_l2,
  prod_hier_l3,
  prod_hier_l4,
  prod_hier_l5,
  prod_hier_l6,
  prod_hier_l7,
  prod_hier_l8,
  prod_hier_l9,
  ques_type,
  "y/n_flag",
  priority_store_flag,
  add_info,
  response,
  response_score,
  kpi_chnl_wt,
  channel_weightage,
  weight_msl,
  weight_oos,
  weight_soa,
  weight_sos,
  weight_promo,
  weight_planogram,
  weight_display,
  mkt_share,
  salience_val,
  actual_value,
  ref_value,
  KPI_ACTUAL_WT_VAL, --- new column addition as part of India GT
  KPI_REF_VAL, --- new column addition as part of India GT
  KPI_REF_WT_VAL, --- new column addition as part of India GT
  PHOTO_URL,
       STORE_GRADE
  --gcph_category,
  --gcph_subcategory
)
SELECT hashkey,
       hash_row,
       dataset,
       customerid,
       salespersonid,
       visitid,
       questiontext,
       productid,
       kpi,
       scheduleddate,
       latestdate,
       fisc_yr,
       fisc_per,
       merchandiser_name,
       customername,
       country,
       state,
       parent_customer,
       retail_environment,
       channel,
       retailer,
       business_unit,
       eannumber,
       matl_num,
       prod_hier_l1,
       prod_hier_l2,
       prod_hier_l3,
       prod_hier_l4,
       prod_hier_l5,
       prod_hier_l6,
       prod_hier_l7,
       prod_hier_l8,
       prod_hier_l9,
       ques_type,
       "y/n_flag",
       priority_store_flag,
       add_info,
       response,
       response_score,
       kpi_chnl_wt,
       channel_weightage,
       weight_msl,
       weight_oos,
       weight_soa,
       weight_sos,
       weight_promo,
       weight_planogram,
       weight_display,
       mkt_share,
       salience_val,
       actual_value,
       ref_value,
	   KPI_ACTUAL_WT_VAL,
	   KPI_REF_VAL,
	   KPI_REF_WT_VAL,
	   PHOTO_URL,
       STORE_GRADE
	   --gcph_category,
  --gcph_subcategory
FROM RG_EDW.EDW_PERFECT_STORE_KPI_DATA;


--- Insert data of aggregated Perfect store KPI data ---

------- MSL Data ---------
INSERT INTO RG_EDW.EDW_PERFECT_STORE_REBASE_WT
(
  DATASET,
  CUSTOMERID,
  SALESPERSONID,
  KPI,
  SCHEDULEDDATE,
  LATESTDATE,
  FISC_YR,
  FISC_PER,
  MERCHANDISER_NAME,
  CUSTOMERNAME,
  COUNTRY,
  STATE,
  PARENT_CUSTOMER,
  RETAIL_ENVIRONMENT,
  CHANNEL,
  RETAILER,
  BUSINESS_UNIT,
  PRIORITY_STORE_FLAG,
  KPI_CHNL_WT,
  CHANNEL_WEIGHTAGE,
  SALIENCE_VAL,
  ACTUAL_VALUE,
  REF_VALUE,
  KPI_ACTUAL_WT_VAL,
  KPI_REF_VAL,
  KPI_REF_WT_VAL,
  PHOTO_URL,
  STORE_GRADE
)
SELECT DATASET,
       CUSTOMERID,
       SALESPERSONID,
       KPI,
       TO_DATE(YM|| '15','YYYYMMDD') AS SCHEDULEDDATE,
       LATESTDATE,
       FISC_YR,
       FISC_PER,
       MERCHANDISER_NAME,
       CUSTOMERNAME,
       COUNTRY,
       STATE,
       PARENT_CUSTOMER,
       RETAIL_ENVIRONMENT,
       CHANNEL,
       RETAILER,
       BUSINESS_UNIT,
       PRIORITY_STORE_FLAG,
       KPI_CHNL_WT,
       CHANNEL_WEIGHTAGE,
       SALIENCE_VAL,
       ACTUAL_VALUE,
       REF_VALUE,
       KPI_ACTUAL_WT_VAL,
       KPI_REF_VAL,
       KPI_REF_WT_VAL,
	   PHOTO_URL,
  STORE_GRADE
FROM (SELECT DATASET,
             CUSTOMERID,
             SALESPERSONID,
             KPI,
             TO_CHAR(SCHEDULEDDATE,'YYYYMM') AS YM,
             LATESTDATE,
             FISC_YR,
             FISC_PER,
             MERCHANDISER_NAME,
             CUSTOMERNAME,
             COUNTRY,
             STATE,
             PARENT_CUSTOMER,
             RETAIL_ENVIRONMENT,
             CHANNEL,
             RETAILER,
             BUSINESS_UNIT,
             PRIORITY_STORE_FLAG,
             MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
             MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
             MAX(SALIENCE_VAL) AS SALIENCE_VAL,
             SUM(ACTUAL_VALUE) AS ACTUAL_VALUE,
             SUM(REF_VALUE) AS REF_VALUE,
             SUM(KPI_ACTUAL_WT_VAL) AS KPI_ACTUAL_WT_VAL,
             SUM(KPI_REF_VAL) AS KPI_REF_VAL,
             SUM(KPI_REF_WT_VAL) AS KPI_REF_WT_VAL,
			 PHOTO_URL,
  STORE_GRADE
      FROM (SELECT 'MSL COMPLIANCE' AS DATASET,
                   CUSTOMERID,
                   SALESPERSONID,
                   'PERFECT STORE SCORE' AS KPI,
                   SCHEDULEDDATE,
                   LATESTDATE,
                   FISC_YR,
                   FISC_PER,
                   MERCHANDISER_NAME,
                   CUSTOMERNAME,
                   COUNTRY,
                   STATE,
                   PARENT_CUSTOMER,
                   RETAIL_ENVIRONMENT,
                   CHANNEL,
                   RETAILER,
                   BUSINESS_UNIT,
                   PRIORITY_STORE_FLAG,
                   KPI_CHNL_WT,
                   CHANNEL_WEIGHTAGE,
                   SALIENCE_VAL,
                   ACTUAL_VALUE,
                   REF_VALUE,
                   ROUND(ACTUAL_VALUE*WEIGHT_MSL,4) AS KPI_ACTUAL_WT_VAL,
                   REF_VALUE AS KPI_REF_VAL,
                   ROUND(REF_VALUE*WEIGHT_MSL,4) AS KPI_REF_WT_VAL,
				   PHOTO_URL,
                   STORE_GRADE
            FROM RG_EDW.EDW_PERFECT_STORE_KPI_DATA
            WHERE UPPER(KPI) = 'MSL COMPLIANCE'
			AND   UPPER(COUNTRY) <> 'INDIA')
      GROUP BY DATASET,
               CUSTOMERID,
               SALESPERSONID,
               KPI,
               TO_CHAR(SCHEDULEDDATE,'YYYYMM'),
               LATESTDATE,
               FISC_YR,
               FISC_PER,
               MERCHANDISER_NAME,
               CUSTOMERNAME,
               COUNTRY,
               STATE,
               PARENT_CUSTOMER,
               RETAIL_ENVIRONMENT,
               CHANNEL,
               RETAILER,
               BUSINESS_UNIT,
               PRIORITY_STORE_FLAG,
			   PHOTO_URL,
               STORE_GRADE;
			   
INSERT INTO RG_EDW.EDW_PERFECT_STORE_REBASE_WT
(
  DATASET,
  CUSTOMERID,
  SALESPERSONID,
  KPI,
  SCHEDULEDDATE,
  LATESTDATE,
  FISC_YR,
  FISC_PER,
  MERCHANDISER_NAME,
  CUSTOMERNAME,
  COUNTRY,
  STATE,
  PARENT_CUSTOMER,
  RETAIL_ENVIRONMENT,
  CHANNEL,
  RETAILER,
  BUSINESS_UNIT,
  PRIORITY_STORE_FLAG,
  KPI_CHNL_WT,
  CHANNEL_WEIGHTAGE,
  SALIENCE_VAL,
  ACTUAL_VALUE,
  REF_VALUE,
  KPI_ACTUAL_WT_VAL,
  KPI_REF_VAL,
  KPI_REF_WT_VAL,
  PHOTO_URL,
  STORE_GRADE
)
SELECT DATASET,
       CUSTOMERID,
       SALESPERSONID,
       KPI,
       TO_DATE(YM|| '15','YYYYMMDD') AS SCHEDULEDDATE,
       LATESTDATE,
       FISC_YR,
       FISC_PER,
       MERCHANDISER_NAME,
       CUSTOMERNAME,
       COUNTRY,
       STATE,
       PARENT_CUSTOMER,
       RETAIL_ENVIRONMENT,
       CHANNEL,
       RETAILER,
       BUSINESS_UNIT,
       PRIORITY_STORE_FLAG,
       KPI_CHNL_WT,
       CHANNEL_WEIGHTAGE,
       SALIENCE_VAL,
       ACTUAL_VALUE,
       REF_VALUE,
       KPI_ACTUAL_WT_VAL,
       KPI_REF_VAL,
       KPI_REF_WT_VAL,
	   PHOTO_URL,
       STORE_GRADE
FROM (SELECT DATASET,
             CUSTOMERID,
             SALESPERSONID,
             KPI,
             TO_CHAR(SCHEDULEDDATE,'YYYYMM') AS YM,
             LATESTDATE,
             FISC_YR,
             FISC_PER,
             MERCHANDISER_NAME,
             CUSTOMERNAME,
             COUNTRY,
             STATE,
             PARENT_CUSTOMER,
             RETAIL_ENVIRONMENT,
             CHANNEL,
             RETAILER,
             BUSINESS_UNIT,
             PRIORITY_STORE_FLAG,
             MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
             MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
             MAX(SALIENCE_VAL) AS SALIENCE_VAL,
             SUM(ACTUAL_VALUE) AS ACTUAL_VALUE,
             SUM(REF_VALUE) AS REF_VALUE,
             SUM(KPI_ACTUAL_WT_VAL) AS KPI_ACTUAL_WT_VAL,
             SUM(KPI_REF_VAL) AS KPI_REF_VAL,
             SUM(KPI_REF_WT_VAL) AS KPI_REF_WT_VAL,
			 PHOTO_URL,
             STORE_GRADE
      FROM (SELECT 'OOS COMPLIANCE' AS DATASET,
                   CUSTOMERID,
                   SALESPERSONID,
                   'PERFECT STORE SCORE' AS KPI,
                   SCHEDULEDDATE,
                   LATESTDATE,
                   FISC_YR,
                   FISC_PER,
                   MERCHANDISER_NAME,
                   CUSTOMERNAME,
                   COUNTRY,
                   STATE,
                   PARENT_CUSTOMER,
                   RETAIL_ENVIRONMENT,
                   CHANNEL,
                   RETAILER,
                   BUSINESS_UNIT,
                   PRIORITY_STORE_FLAG,
                   KPI_CHNL_WT,
                   CHANNEL_WEIGHTAGE,
                   SALIENCE_VAL,
                   CASE
                     WHEN ACTUAL_VALUE = 0 THEN 1
                     ELSE 0
                   END AS ACTUAL_VALUE,
                   REF_VALUE,
                   ROUND((CASE WHEN ACTUAL_VALUE = 0 THEN 1 ELSE 0 END)*WEIGHT_OOS,4) AS KPI_ACTUAL_WT_VAL,
                   REF_VALUE AS KPI_REF_VAL,
                   ROUND(REF_VALUE*WEIGHT_OOS,4) AS KPI_REF_WT_VAL,
				   PHOTO_URL,
                   STORE_GRADE
            FROM RG_EDW.EDW_PERFECT_STORE_KPI_DATA
            WHERE UPPER(KPI) = 'OOS COMPLIANCE')
      GROUP BY DATASET,
               CUSTOMERID,
               SALESPERSONID,
               KPI,
               TO_CHAR(SCHEDULEDDATE,'YYYYMM'),
               LATESTDATE,
               FISC_YR,
               FISC_PER,
               MERCHANDISER_NAME,
               CUSTOMERNAME,
               COUNTRY,
               STATE,
               PARENT_CUSTOMER,
               RETAIL_ENVIRONMENT,
               CHANNEL,
               RETAILER,
               BUSINESS_UNIT,
               PRIORITY_STORE_FLAG,
			   PHOTO_URL,
               STORE_GRADE) OSA;

------- PROMO Data ---------
INSERT INTO RG_EDW.EDW_PERFECT_STORE_REBASE_WT
(
  DATASET,
  CUSTOMERID,
  SALESPERSONID,
  KPI,
  SCHEDULEDDATE,
  LATESTDATE,
  FISC_YR,
  FISC_PER,
  MERCHANDISER_NAME,
  CUSTOMERNAME,
  COUNTRY,
  STATE,
  PARENT_CUSTOMER,
  RETAIL_ENVIRONMENT,
  CHANNEL,
  RETAILER,
  BUSINESS_UNIT,
  PRIORITY_STORE_FLAG,
  KPI_CHNL_WT,
  CHANNEL_WEIGHTAGE,
  SALIENCE_VAL,
  ACTUAL_VALUE,
  REF_VALUE,
  KPI_ACTUAL_WT_VAL,
  KPI_REF_VAL,
  KPI_REF_WT_VAL,
  PHOTO_URL,
  STORE_GRADE
)
SELECT DATASET,
       CUSTOMERID,
       SALESPERSONID,
       KPI,
       TO_DATE(YM|| '15','YYYYMMDD') AS SCHEDULEDDATE,
       LATESTDATE,
       FISC_YR,
       FISC_PER,
       MERCHANDISER_NAME,
       CUSTOMERNAME,
       COUNTRY,
       STATE,
       PARENT_CUSTOMER,
       RETAIL_ENVIRONMENT,
       CHANNEL,
       RETAILER,
       BUSINESS_UNIT,
       PRIORITY_STORE_FLAG,
       KPI_CHNL_WT,
	   CHANNEL_WEIGHTAGE,
       SALIENCE_VAL,
       ACTUAL_VALUE,
       REF_VALUE,
       KPI_ACTUAL_WT_VAL,
       KPI_REF_VAL,
       KPI_REF_WT_VAL,
	   PHOTO_URL,
       STORE_GRADE
FROM (SELECT DATASET,
             CUSTOMERID,
             SALESPERSONID,
             KPI,
             TO_CHAR(SCHEDULEDDATE,'YYYYMM') AS YM,
             LATESTDATE,
             FISC_YR,
             FISC_PER,
             MERCHANDISER_NAME,
             CUSTOMERNAME,
             COUNTRY,
             STATE,
             PARENT_CUSTOMER,
             RETAIL_ENVIRONMENT,
             CHANNEL,
             RETAILER,
             BUSINESS_UNIT,
             PRIORITY_STORE_FLAG,
             MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
			 MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
             MAX(SALIENCE_VAL) AS SALIENCE_VAL,
             SUM(ACTUAL_VALUE) AS ACTUAL_VALUE,
             SUM(REF_VALUE) AS REF_VALUE,
             SUM(KPI_ACTUAL_WT_VAL) AS KPI_ACTUAL_WT_VAL,
             SUM(KPI_REF_VAL) AS KPI_REF_VAL,
             SUM(KPI_REF_WT_VAL) AS KPI_REF_WT_VAL,
			 PHOTO_URL,
             STORE_GRADE
      FROM (SELECT 'PROMO COMPLIANCE' AS DATASET,
                   CUSTOMERID,
                   SALESPERSONID,
                   'PERFECT STORE SCORE' AS KPI,
                   SCHEDULEDDATE,
                   LATESTDATE,
                   FISC_YR,
                   FISC_PER,
                   MERCHANDISER_NAME,
                   CUSTOMERNAME,
                   COUNTRY,
                   STATE,
                   PARENT_CUSTOMER,
                   RETAIL_ENVIRONMENT,
                   CHANNEL,
                   RETAILER,
                   BUSINESS_UNIT,
                   PRIORITY_STORE_FLAG,
                   KPI_CHNL_WT,
				   CHANNEL_WEIGHTAGE,
                   SALIENCE_VAL,
                   ACTUAL_VALUE,
                   REF_VALUE,
                   ROUND(ACTUAL_VALUE*WEIGHT_PROMO,4) AS KPI_ACTUAL_WT_VAL,
                   REF_VALUE AS KPI_REF_VAL,
                   ROUND(REF_VALUE*WEIGHT_PROMO,4) AS KPI_REF_WT_VAL,
				   PHOTO_URL,
                   STORE_GRADE
            FROM RG_EDW.EDW_PERFECT_STORE_KPI_DATA
            WHERE UPPER(KPI) = 'PROMO COMPLIANCE')
      GROUP BY DATASET,
               CUSTOMERID,
               SALESPERSONID,
               KPI,
               TO_CHAR(SCHEDULEDDATE,'YYYYMM'),
               LATESTDATE,
               FISC_YR,
               FISC_PER,
               MERCHANDISER_NAME,
               CUSTOMERNAME,
               COUNTRY,
               STATE,
               PARENT_CUSTOMER,
               RETAIL_ENVIRONMENT,
               CHANNEL,
               RETAILER,
               BUSINESS_UNIT,
               PRIORITY_STORE_FLAG,
			   PHOTO_URL,
               STORE_GRADE) PROMO;

------- DISPLAY Data ---------
INSERT INTO RG_EDW.EDW_PERFECT_STORE_REBASE_WT
(
  DATASET,
  CUSTOMERID,
  SALESPERSONID,
  KPI,
  SCHEDULEDDATE,
  LATESTDATE,
  FISC_YR,
  FISC_PER,
  MERCHANDISER_NAME,
  CUSTOMERNAME,
  COUNTRY,
  STATE,
  PARENT_CUSTOMER,
  RETAIL_ENVIRONMENT,
  CHANNEL,
  RETAILER,
  BUSINESS_UNIT,
  PRIORITY_STORE_FLAG,
  KPI_CHNL_WT,
  CHANNEL_WEIGHTAGE,
  SALIENCE_VAL,
  ACTUAL_VALUE,
  REF_VALUE,
  KPI_ACTUAL_WT_VAL,
  KPI_REF_VAL,
  KPI_REF_WT_VAL,
  PHOTO_URL,
  STORE_GRADE
)
SELECT DATASET,
       CUSTOMERID,
       SALESPERSONID,
       KPI,
       TO_DATE(YM|| '15','YYYYMMDD') AS SCHEDULEDDATE,
       LATESTDATE,
       FISC_YR,
       FISC_PER,
       MERCHANDISER_NAME,
       CUSTOMERNAME,
       COUNTRY,
       STATE,
       PARENT_CUSTOMER,
       RETAIL_ENVIRONMENT,
       CHANNEL,
       RETAILER,
       BUSINESS_UNIT,
       PRIORITY_STORE_FLAG,
       KPI_CHNL_WT,
       CHANNEL_WEIGHTAGE,
       SALIENCE_VAL,
       ACTUAL_VALUE,
       REF_VALUE,
       KPI_ACTUAL_WT_VAL,
       KPI_REF_VAL,
       KPI_REF_WT_VAL,
	   PHOTO_URL,
       STORE_GRADE
FROM (SELECT DATASET,
             CUSTOMERID,
             SALESPERSONID,
             KPI,
             TO_CHAR(SCHEDULEDDATE,'YYYYMM') AS YM,
             LATESTDATE,
             FISC_YR,
             FISC_PER,
             MERCHANDISER_NAME,
             CUSTOMERNAME,
             COUNTRY,
             STATE,
             PARENT_CUSTOMER,
             RETAIL_ENVIRONMENT,
             CHANNEL,
             RETAILER,
             BUSINESS_UNIT,
             PRIORITY_STORE_FLAG,
             MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
             MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
             MAX(SALIENCE_VAL) AS SALIENCE_VAL,
             SUM(ACTUAL_VALUE) AS ACTUAL_VALUE,
             SUM(REF_VALUE) AS REF_VALUE,
             SUM(KPI_ACTUAL_WT_VAL) AS KPI_ACTUAL_WT_VAL,
             SUM(KPI_REF_VAL) AS KPI_REF_VAL,
             SUM(KPI_REF_WT_VAL) AS KPI_REF_WT_VAL,
			 PHOTO_URL,
             STORE_GRADE
      FROM (SELECT 'DISPLAY COMPLIANCE' AS DATASET,
                   CUSTOMERID,
                   SALESPERSONID,
                   'PERFECT STORE SCORE' AS KPI,
                   SCHEDULEDDATE,
                   LATESTDATE,
                   FISC_YR,
                   FISC_PER,
                   MERCHANDISER_NAME,
                   CUSTOMERNAME,
                   COUNTRY,
                   STATE,
                   PARENT_CUSTOMER,
                   RETAIL_ENVIRONMENT,
                   CHANNEL,
                   RETAILER,
                   BUSINESS_UNIT,
                   PRIORITY_STORE_FLAG,
                   KPI_CHNL_WT,
                   CHANNEL_WEIGHTAGE,
                   SALIENCE_VAL,
                   ACTUAL_VALUE,
                   REF_VALUE,
                   ROUND(ACTUAL_VALUE*WEIGHT_DISPLAY,4) AS KPI_ACTUAL_WT_VAL,
                   REF_VALUE AS KPI_REF_VAL,
                   ROUND(REF_VALUE*WEIGHT_DISPLAY,4) AS KPI_REF_WT_VAL,
				   PHOTO_URL,
                   STORE_GRADE
            FROM RG_EDW.EDW_PERFECT_STORE_KPI_DATA
            WHERE UPPER(KPI) = 'DISPLAY COMPLIANCE')
      GROUP BY DATASET,
               CUSTOMERID,
               SALESPERSONID,
               KPI,
               TO_CHAR(SCHEDULEDDATE,'YYYYMM'),
               LATESTDATE,
               FISC_YR,
               FISC_PER,
               MERCHANDISER_NAME,
               CUSTOMERNAME,
               COUNTRY,
               STATE,
               PARENT_CUSTOMER,
               RETAIL_ENVIRONMENT,
               CHANNEL,
               RETAILER,
               BUSINESS_UNIT,
               PRIORITY_STORE_FLAG,
			   PHOTO_URL,
               STORE_GRADE) DISPLAY;

INSERT INTO RG_EDW.EDW_PERFECT_STORE_REBASE_WT
(
  DATASET,
  CUSTOMERID,
  SALESPERSONID,
  KPI,
  SCHEDULEDDATE,
  LATESTDATE,
  FISC_YR,
  FISC_PER,
  MERCHANDISER_NAME,
  CUSTOMERNAME,
  COUNTRY,
  STATE,
  PARENT_CUSTOMER,
  RETAIL_ENVIRONMENT,
  CHANNEL,
  RETAILER,
  BUSINESS_UNIT,
  PRIORITY_STORE_FLAG,
  KPI_CHNL_WT,
  CHANNEL_WEIGHTAGE,
  SALIENCE_VAL,
  ACTUAL_VALUE,
  REF_VALUE,
  KPI_ACTUAL_WT_VAL,
  KPI_REF_VAL,
  KPI_REF_WT_VAL,
  PHOTO_URL
)
SELECT DATASET,
       CUSTOMERID,
       SALESPERSONID,
       KPI,
       TO_DATE(YM|| '15','YYYYMMDD') AS SCHEDULEDDATE,
       LATESTDATE,
       FISC_YR,
       FISC_PER,
       MERCHANDISER_NAME,
       CUSTOMERNAME,
       COUNTRY,
       STATE,
       PARENT_CUSTOMER,
       RETAIL_ENVIRONMENT,
       CHANNEL,
       RETAILER,
       BUSINESS_UNIT,
       PRIORITY_STORE_FLAG,
       KPI_CHNL_WT,
       CHANNEL_WEIGHTAGE,
       SALIENCE_VAL,
       ACTUAL_VALUE,
       REF_VALUE,
       KPI_ACTUAL_WT_VAL,
       KPI_REF_VAL,
       KPI_REF_WT_VAL,
	   PHOTO_URL
FROM (SELECT DATASET,
             CUSTOMERID,
             SALESPERSONID,
             KPI,
             TO_CHAR(SCHEDULEDDATE,'YYYYMM') AS YM,
             LATESTDATE,
             FISC_YR,
             FISC_PER,
             MERCHANDISER_NAME,
             CUSTOMERNAME,
             COUNTRY,
             STATE,
             PARENT_CUSTOMER,
             RETAIL_ENVIRONMENT,
             CHANNEL,
             RETAILER,
             BUSINESS_UNIT,
             PRIORITY_STORE_FLAG,
             MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
             MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
             MAX(SALIENCE_VAL) AS SALIENCE_VAL,
             SUM(ACTUAL_VALUE) AS ACTUAL_VALUE,
             SUM(REF_VALUE) AS REF_VALUE,
             SUM(KPI_ACTUAL_WT_VAL) AS KPI_ACTUAL_WT_VAL,
             SUM(KPI_REF_VAL) AS KPI_REF_VAL,
             SUM(KPI_REF_WT_VAL) AS KPI_REF_WT_VAL,
			 PHOTO_URL
      FROM (SELECT 'PLANOGRAM COMPLIANCE' AS DATASET,
                   CUSTOMERID,
                   SALESPERSONID,
                   'PERFECT STORE SCORE' AS KPI,
                   SCHEDULEDDATE,
                   LATESTDATE,
                   FISC_YR,
                   FISC_PER,
                   MERCHANDISER_NAME,
                   CUSTOMERNAME,
                   COUNTRY,
                   STATE,
                   PARENT_CUSTOMER,
                   RETAIL_ENVIRONMENT,
                   CHANNEL,
                   RETAILER,
                   BUSINESS_UNIT,
                   PRIORITY_STORE_FLAG,
                   KPI_CHNL_WT,
                   CHANNEL_WEIGHTAGE,
                   SALIENCE_VAL,
                   ACTUAL_VALUE,
                   REF_VALUE,
                   ROUND(ACTUAL_VALUE*WEIGHT_PLANOGRAM,4) AS KPI_ACTUAL_WT_VAL,
                   REF_VALUE AS KPI_REF_VAL,
                   ROUND(REF_VALUE*WEIGHT_PLANOGRAM,4) AS KPI_REF_WT_VAL,
				   PHOTO_URL
            FROM RG_EDW.EDW_PERFECT_STORE_KPI_DATA
            WHERE UPPER(KPI) = 'PLANOGRAM COMPLIANCE'
            AND   UPPER(COUNTRY) NOT IN ('AUSTRALIA','NEW ZEALAND'))
      GROUP BY DATASET,
               CUSTOMERID,
               SALESPERSONID,
               KPI,
               TO_CHAR(SCHEDULEDDATE,'YYYYMM'),
               LATESTDATE,
               FISC_YR,
               FISC_PER,
               MERCHANDISER_NAME,
               CUSTOMERNAME,
               COUNTRY,
               STATE,
               PARENT_CUSTOMER,
               RETAIL_ENVIRONMENT,
               CHANNEL,
               RETAILER,
               BUSINESS_UNIT,
               PRIORITY_STORE_FLAG,
			   PHOTO_URL) POG_Others;

------- SOS Data ---------
INSERT INTO RG_EDW.EDW_PERFECT_STORE_REBASE_WT
(
  DATASET,
  CUSTOMERID,
  SALESPERSONID,
  KPI,
  SCHEDULEDDATE,
  LATESTDATE,
  FISC_YR,
  FISC_PER,
  MERCHANDISER_NAME,
  CUSTOMERNAME,
  COUNTRY,
  STATE,
  PARENT_CUSTOMER,
  RETAIL_ENVIRONMENT,
  CHANNEL,
  RETAILER,
  BUSINESS_UNIT,
  PRIORITY_STORE_FLAG,
  KPI_CHNL_WT,
  CHANNEL_WEIGHTAGE,
  SALIENCE_VAL,
  ACTUAL_VALUE,
  REF_VALUE,
  KPI_ACTUAL_WT_VAL,
  KPI_REF_VAL,
  KPI_REF_WT_VAL,
  PHOTO_URL,
  STORE_GRADE
)
SELECT DATASET,
       CUSTOMERID,
       SALESPERSONID,
       KPI,
       TO_DATE(YM|| '15','YYYYMMDD') AS SCHEDULEDDATE,
       LATESTDATE,
       FISC_YR,
       FISC_PER,
       MERCHANDISER_NAME,
       CUSTOMERNAME,
       COUNTRY,
       STATE,
       PARENT_CUSTOMER,
       RETAIL_ENVIRONMENT,
       CHANNEL,
       RETAILER,
       BUSINESS_UNIT,
       PRIORITY_STORE_FLAG,
       KPI_CHNL_WT,
       CHANNEL_WEIGHTAGE,
       SALIENCE_VAL,
       ACTUAL_VALUE,
       REF_VALUE,
       KPI_ACTUAL_WT_VAL,
       KPI_REF_VAL,
       KPI_REF_WT_VAL,
	   PHOTO_URL,
       STORE_GRADE
FROM (SELECT DATASET,
             CUSTOMERID,
             SALESPERSONID,
             KPI,
             TO_CHAR(SCHEDULEDDATE,'YYYYMM') AS YM,
             LATESTDATE,
             FISC_YR,
             FISC_PER,
             MERCHANDISER_NAME,
             CUSTOMERNAME,
             COUNTRY,
             STATE,
             PARENT_CUSTOMER,
             RETAIL_ENVIRONMENT,
             CHANNEL,
             RETAILER,
             BUSINESS_UNIT,
             PRIORITY_STORE_FLAG,
             MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
             MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
             MAX(SALIENCE_VAL) AS SALIENCE_VAL,
             SUM(NUM_VALUE) AS ACTUAL_VALUE,
             SUM(DEN_VALUE) AS REF_VALUE,
             SUM(NUM_VALUE*WEIGHT_SOS) AS KPI_ACTUAL_WT_VAL,
             SUM(DEN_VALUE) AS KPI_REF_VAL,
             SUM(DEN_VALUE*WEIGHT_SOS) AS KPI_REF_WT_VAL,
			 PHOTO_URL,
             STORE_GRADE
      FROM (SELECT DATASET,
                   CUSTOMERID,
                   SALESPERSONID,
                   KPI,
                   TRANS.SCHEDULEDDATE,
                   LATESTDATE,
                   FISC_YR,
                   FISC_PER,
                   MERCHANDISER_NAME,
                   TRANS.CUSTOMERNAME,
                   TRANS.COUNTRY,
                   STATE,
                   PARENT_CUSTOMER,
                   RETAIL_ENVIRONMENT,
                   CHANNEL,
                   RETAILER,
                   BUSINESS_UNIT,
                   PRIORITY_STORE_FLAG,
                   TRANS.PROD_HIER_L4,
                   TRANS.PROD_HIER_L5,
                   MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
                   MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
                   MAX(WEIGHT_SOS) AS WEIGHT_SOS,
                   MAX(SALIENCE_VAL) AS SALIENCE_VAL,
                   SUM(NUM_VALUE) AS NUM_VALUE,
                   SUM(DEN_VALUE) AS DEN_VALUE,
				   PHOTO_URL,
                   STORE_GRADE
            FROM (SELECT 'SOS COMPLIANCE' AS DATASET,
                         CUSTOMERID,
                         SALESPERSONID,
                         'PERFECT STORE SCORE' AS KPI,
                         SCHEDULEDDATE,
                         LATESTDATE,
                         FISC_YR,
                         FISC_PER,
                         MERCHANDISER_NAME,
                         CUSTOMERNAME,
                         COUNTRY,
                         STATE,
                         PARENT_CUSTOMER,
                         RETAIL_ENVIRONMENT,
                         CHANNEL,
                         RETAILER,
                         BUSINESS_UNIT,
                         PRIORITY_STORE_FLAG,
                         KPI_CHNL_WT,
                         CHANNEL_WEIGHTAGE,
                         ROUND(WEIGHT_SOS,4) AS WEIGHT_SOS,
                         SALIENCE_VAL,
                         PROD_HIER_L4,
                         PROD_HIER_L5,
                         QUES_TYPE,
                         CASE
                           WHEN UPPER(QUES_TYPE) = 'NUMERATOR' AND MKT_SHARE IS NOT NULL THEN ACTUAL_VALUE
                           ELSE NULL
                         END AS NUM_VALUE,
                         CASE
                           WHEN UPPER(QUES_TYPE) = 'DENOMINATOR' THEN ROUND(ACTUAL_VALUE*MKT_SHARE,4)
                           ELSE NULL
                         END AS DEN_VALUE,
						 PHOTO_URL,
                         STORE_GRADE
                  FROM RG_EDW.EDW_PERFECT_STORE_KPI_DATA
                  WHERE UPPER(KPI) = 'SOS COMPLIANCE'
                  AND   (ACTUAL_VALUE ~ '^[0-9]+\\.[0-9]+$' OR ACTUAL_VALUE ~ '^[0-9]+$')) TRANS
              INNER JOIN (SELECT DISTINCT COUNTRY,
                                 CUSTOMERNAME,
                                 SCHEDULEDDATE,
                                 PROD_HIER_L4,
                                 PROD_HIER_L5
                          FROM RG_EDW.EDW_PERFECT_STORE_KPI_DATA
                          WHERE UPPER(KPI) = 'SOS COMPLIANCE'
                          AND   (ACTUAL_VALUE ~ '^[0-9]+\\.[0-9]+$' OR ACTUAL_VALUE ~ '^[0-9]+$')
                          GROUP BY COUNTRY,
                                   CUSTOMERNAME,
                                   SCHEDULEDDATE,
                                   PROD_HIER_L4,
                                   PROD_HIER_L5
                          HAVING COUNT(DISTINCT QUES_TYPE) = 2) NUM_DEN
                      ON NVL (NUM_DEN.COUNTRY,'X') = NVL (TRANS.COUNTRY,'X')
                     AND NVL (NUM_DEN.CUSTOMERNAME,'X') = NVL (TRANS.CUSTOMERNAME,'X')
                     AND NUM_DEN.SCHEDULEDDATE = TRANS.SCHEDULEDDATE
                     AND NVL (NUM_DEN.PROD_HIER_L4,'X') = NVL (TRANS.PROD_HIER_L4,'X')
                     AND NVL (NUM_DEN.PROD_HIER_L5,'X') = NVL (TRANS.PROD_HIER_L5,'X')
            GROUP BY DATASET,
                     CUSTOMERID,
                     SALESPERSONID,
                     KPI,
                     TRANS.SCHEDULEDDATE,
                     LATESTDATE,
                     FISC_YR,
                     FISC_PER,
                     MERCHANDISER_NAME,
                     TRANS.CUSTOMERNAME,
                     TRANS.COUNTRY,
                     STATE,
                     PARENT_CUSTOMER,
                     RETAIL_ENVIRONMENT,
                     CHANNEL,
                     RETAILER,
                     BUSINESS_UNIT,
                     PRIORITY_STORE_FLAG,
                     TRANS.PROD_HIER_L4,
                     TRANS.PROD_HIER_L5,
					 PHOTO_URL,
                     STORE_GRADE)
      GROUP BY DATASET,
               CUSTOMERID,
               SALESPERSONID,
               KPI,
               TO_CHAR(SCHEDULEDDATE,'YYYYMM'),
               LATESTDATE,
               FISC_YR,
               FISC_PER,
               MERCHANDISER_NAME,
               CUSTOMERNAME,
               COUNTRY,
               STATE,
               PARENT_CUSTOMER,
               RETAIL_ENVIRONMENT,
               CHANNEL,
               RETAILER,
               BUSINESS_UNIT,
               PRIORITY_STORE_FLAG,
			   PHOTO_URL,
               STORE_GRADE) SOS;

------- SOA Data ---------
INSERT INTO RG_EDW.EDW_PERFECT_STORE_REBASE_WT
(
  DATASET,
  CUSTOMERID,
  SALESPERSONID,
  KPI,
  SCHEDULEDDATE,
  LATESTDATE,
  FISC_YR,
  FISC_PER,
  MERCHANDISER_NAME,
  CUSTOMERNAME,
  COUNTRY,
  STATE,
  PARENT_CUSTOMER,
  RETAIL_ENVIRONMENT,
  CHANNEL,
  RETAILER,
  BUSINESS_UNIT,
  PRIORITY_STORE_FLAG,
  KPI_CHNL_WT,
  CHANNEL_WEIGHTAGE,
  SALIENCE_VAL,
  ACTUAL_VALUE,
  REF_VALUE,
  KPI_ACTUAL_WT_VAL,
  KPI_REF_VAL,
  KPI_REF_WT_VAL,
  PHOTO_URL,
  STORE_GRADE
)
SELECT DATASET,
       CUSTOMERID,
       SALESPERSONID,
       KPI,
       TO_DATE(YM|| '15','YYYYMMDD') AS SCHEDULEDDATE,
       LATESTDATE,
       FISC_YR,
       FISC_PER,
       MERCHANDISER_NAME,
       CUSTOMERNAME,
       COUNTRY,
       STATE,
       PARENT_CUSTOMER,
       RETAIL_ENVIRONMENT,
       CHANNEL,
       RETAILER,
       BUSINESS_UNIT,
       PRIORITY_STORE_FLAG,
       KPI_CHNL_WT,
       CHANNEL_WEIGHTAGE,
       SALIENCE_VAL,
       ACTUAL_VALUE,
       REF_VALUE,
       KPI_ACTUAL_WT_VAL,
       KPI_REF_VAL,
       KPI_REF_WT_VAL,
	   PHOTO_URL,
       STORE_GRADE
FROM (SELECT DATASET,
             CUSTOMERID,
             SALESPERSONID,
             KPI,
             TO_CHAR(SCHEDULEDDATE,'YYYYMM') AS YM,
             LATESTDATE,
             FISC_YR,
             FISC_PER,
             MERCHANDISER_NAME,
             CUSTOMERNAME,
             COUNTRY,
             STATE,
             PARENT_CUSTOMER,
             RETAIL_ENVIRONMENT,
             CHANNEL,
             RETAILER,
             BUSINESS_UNIT,
             PRIORITY_STORE_FLAG,
             MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
             MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
             MAX(SALIENCE_VAL) AS SALIENCE_VAL,
             SUM(NUM_VALUE) AS ACTUAL_VALUE,
             SUM(DEN_VALUE) AS REF_VALUE,
             SUM(NUM_VALUE*WEIGHT_SOA) AS KPI_ACTUAL_WT_VAL,
             SUM(DEN_VALUE) AS KPI_REF_VAL,
             SUM(DEN_VALUE*WEIGHT_SOA) AS KPI_REF_WT_VAL,
			 PHOTO_URL,
             STORE_GRADE
      FROM (SELECT DATASET,
                   CUSTOMERID,
                   SALESPERSONID,
                   KPI,
                   TRANS.SCHEDULEDDATE,
                   LATESTDATE,
                   FISC_YR,
                   FISC_PER,
                   MERCHANDISER_NAME,
                   TRANS.CUSTOMERNAME,
                   TRANS.COUNTRY,
                   STATE,
                   PARENT_CUSTOMER,
                   RETAIL_ENVIRONMENT,
                   CHANNEL,
                   RETAILER,
                   BUSINESS_UNIT,
                   PRIORITY_STORE_FLAG,
                   TRANS.PROD_HIER_L4,
                   TRANS.PROD_HIER_L5,
                   MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
                   MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
                   MAX(WEIGHT_SOA) AS WEIGHT_SOA,
                   MAX(SALIENCE_VAL) AS SALIENCE_VAL,
                   SUM(NUM_VALUE) AS NUM_VALUE,
                   SUM(DEN_VALUE) AS DEN_VALUE,
				   PHOTO_URL,
                   STORE_GRADE
            FROM (SELECT 'SOA COMPLIANCE' AS DATASET,
                         CUSTOMERID,
                         SALESPERSONID,
                         'PERFECT STORE SCORE' AS KPI,
                         SCHEDULEDDATE,
                         LATESTDATE,
                         FISC_YR,
                         FISC_PER,
                         MERCHANDISER_NAME,
                         CUSTOMERNAME,
                         COUNTRY,
                         STATE,
                         PARENT_CUSTOMER,
                         RETAIL_ENVIRONMENT,
                         CHANNEL,
                         RETAILER,
                         BUSINESS_UNIT,
                         PRIORITY_STORE_FLAG,
                         KPI_CHNL_WT,
                         CHANNEL_WEIGHTAGE,
                         ROUND(WEIGHT_SOA,4) AS WEIGHT_SOA,
                         SALIENCE_VAL,
                         PROD_HIER_L4,
                         PROD_HIER_L5,
                         QUES_TYPE,
                         CASE
                           WHEN UPPER(QUES_TYPE) = 'NUMERATOR' AND MKT_SHARE IS NOT NULL THEN ACTUAL_VALUE
                           ELSE NULL
                         END AS NUM_VALUE,
                         CASE
                           WHEN UPPER(QUES_TYPE) = 'DENOMINATOR' THEN ROUND(ACTUAL_VALUE*MKT_SHARE,4)
                           ELSE NULL
                         END AS DEN_VALUE,
						 PHOTO_URL,
                         STORE_GRADE
                  FROM RG_EDW.EDW_PERFECT_STORE_KPI_DATA
                  WHERE UPPER(KPI) = 'SOA COMPLIANCE'
                  AND   (ACTUAL_VALUE ~ '^[0-9]+\\.[0-9]+$' OR ACTUAL_VALUE ~ '^[0-9]+$')) TRANS
              INNER JOIN (SELECT DISTINCT COUNTRY,
                                 CUSTOMERNAME,
                                 SCHEDULEDDATE,
                                 PROD_HIER_L4,
                                 PROD_HIER_L5
                          FROM RG_EDW.EDW_PERFECT_STORE_KPI_DATA
                          WHERE UPPER(KPI) = 'SOA COMPLIANCE'
                          AND   (ACTUAL_VALUE ~ '^[0-9]+\\.[0-9]+$' OR ACTUAL_VALUE ~ '^[0-9]+$')
                          GROUP BY COUNTRY,
                                   CUSTOMERNAME,
                                   SCHEDULEDDATE,
                                   PROD_HIER_L4,
                                   PROD_HIER_L5
                          HAVING COUNT(DISTINCT QUES_TYPE) = 2) NUM_DEN
                      ON NVL (NUM_DEN.COUNTRY,'X') = NVL (TRANS.COUNTRY,'X')
                     AND NVL (NUM_DEN.CUSTOMERNAME,'X') = NVL (TRANS.CUSTOMERNAME,'X')
                     AND NUM_DEN.SCHEDULEDDATE = TRANS.SCHEDULEDDATE
                     AND NVL (NUM_DEN.PROD_HIER_L4,'X') = NVL (TRANS.PROD_HIER_L4,'X')
                     AND NVL (NUM_DEN.PROD_HIER_L5,'X') = NVL (TRANS.PROD_HIER_L5,'X')
            GROUP BY DATASET,
                     CUSTOMERID,
                     SALESPERSONID,
                     KPI,
                     TRANS.SCHEDULEDDATE,
                     LATESTDATE,
                     FISC_YR,
                     FISC_PER,
                     MERCHANDISER_NAME,
                     TRANS.CUSTOMERNAME,
                     TRANS.COUNTRY,
                     STATE,
                     PARENT_CUSTOMER,
                     RETAIL_ENVIRONMENT,
                     CHANNEL,
                     RETAILER,
                     BUSINESS_UNIT,
                     PRIORITY_STORE_FLAG,
                     TRANS.PROD_HIER_L4,
                     TRANS.PROD_HIER_L5,
					 PHOTO_URL,
                     STORE_GRADE)
      GROUP BY DATASET,
               CUSTOMERID,
               SALESPERSONID,
               KPI,
               TO_CHAR(SCHEDULEDDATE,'YYYYMM'),
               LATESTDATE,
               FISC_YR,
               FISC_PER,
               MERCHANDISER_NAME,
               CUSTOMERNAME,
               COUNTRY,
               STATE,
               PARENT_CUSTOMER,
               RETAIL_ENVIRONMENT,
               CHANNEL,
               RETAILER,
               BUSINESS_UNIT,
               PRIORITY_STORE_FLAG,
			   PHOTO_URL,
               STORE_GRADE) SOA;			   
			   