{{
    config(
        pre_hook="{{build_pcfedw_integration__edw_gts_visibility()}}"
    )
}}


with 
edw_invoice_fact_snapshot as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_INVOICE_FACT_SNAPSHOT
),
edw_billing_fact as (
select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_BILLING_FACT
),
EDW_BILLING_FACT_SNAPSHOT as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_BILLING_FACT_SNAPSHOT
),
EDW_SAPBW_PLAN_LKP as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_SAPBW_PLAN_LKP
),
edw_time_dim as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_TIME_DIM
),
vw_jjbr_curr_exch_dim as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.VW_JJBR_CURR_EXCH_DIM
),
vw_bwar_curr_exch_dim as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.VW_BWAR_CURR_EXCH_DIM
),
vw_customer_dim as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.VW_CUSTOMER_DIM
),
edw_material_dim as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_MATERIAL_DIM
),
vw_apo_parent_child_dim as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.VW_APO_PARENT_CHILD_DIM
),
edw_gch_producthierarchy as (
select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_GCH_PRODUCTHIERARCHY
),
itg_query_parameters as (
select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_QUERY_PARAMETERS
),
edw_invoice_fact as (
select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_INVOICE_FACT
),
dly_sls_cust_attrb_lkp as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.DLY_SLS_CUST_ATTRB_LKP
),
WKS_GTS_VISIBILITY as (
select case

         when EIFS.CO_CD = '7470' then 'Australia'

         when EIFS.CO_CD = '8361' then 'New Zealand'

         else null

       end as country,

       eifs.snapshot_date,

       eifs.jj_mnth_id,

       etd.jj_mnth,

       etd.jj_mnth_shrt as jj_mnth_nm,

       etd.jj_year,

       etd.jj_qrtr,

       ltrim(eifs.cust_num) as cust_num,

       ltrim(eifs.matl_num) as matl_num,

       eifs.sls_doc,

       ebf.bill_num,

       ebf.bill_dt,

       ebf.created_on,

       emd.grp_fran_desc,

       emd.prod_fran_desc,

       emd.prod_mjr_desc,

       emd.prod_mnr_desc,

       emd.matl_desc,

       emd.brnd_desc,

       egph.gcph_franchise,

       egph.gcph_brand,

       egph.gcph_subbrand,

       egph.gcph_variant,

       egph.gcph_needstate,

       egph.gcph_category,

       egph.gcph_subcategory,

       egph.gcph_segment,

       egph.gcph_subsegment,

       mstrcd.master_code,

       vcd.channel_desc,

       vcd.sales_office_desc,

       vcd.cust_nm,

       vcd.sales_grp_desc,

       eifs.curr_key as local_ccy,

       currency.to_ccy as to_ccy,

       currency.exch_rate as exch_rate,

       (eifs.gros_trd_sls*currency.exch_rate) as open_orders_val,

       (ebf.subtotal_1*currency.exch_rate) as gts_landing_val

from edw_invoice_fact_snapshot eifs

  left join (select doc_num,

                    sold_to,

                    material,

                    bill_num,

                    bill_dt,

                    created_on,

                    sum(subtotal_1) as subtotal_1

             from edw_billing_fact

             where  bill_type not in (select parameter_value from itg_query_parameters where country_code='ANZ' and parameter_name='Billing_Exclusion' and parameter_type='bill_type')

             group by doc_num,

                      sold_to,

                      material,

                      bill_num,

                      bill_dt,

                      created_on) ebf

         on ltrim (eifs.sls_doc,'0') = ltrim (ebf.doc_num,'0')

        and ltrim (eifs.cust_num,'0') = ltrim (ebf.sold_to,'0')

        and ltrim (eifs.matl_num,'0') = ltrim (ebf.material,'0')

  join (select distinct jj_mnth,

               jj_mnth_shrt,

               jj_qrtr,

               jj_year,

               jj_mnth_id

        from edw_time_dim) etd on eifs.jj_mnth_id = etd.jj_mnth_id

  join (select rate_type,

               from_ccy,

               to_ccy,

               jj_mnth_id,

               exch_rate

        from vw_jjbr_curr_exch_dim

        where exch_rate = 1

        and   from_ccy = 'AUD'

        union all

        select rate_type,

               from_ccy,

               to_ccy,

               jj_mnth_id,

               exch_rate

        from vw_bwar_curr_exch_dim) currency

    on eifs.curr_key = currency.from_ccy

   and etd.jj_mnth_id = currency.jj_mnth_id

  left join vw_customer_dim vcd on ltrim (eifs.cust_num,'0') = ltrim (vcd.cust_no,'0')

  left join edw_material_dim emd on ltrim (eifs.matl_num,'0') = ltrim (emd.matl_id,'0')

  left join (vw_apo_parent_child_dim vapcd

  left join (select distinct master_code,

                    parent_matl_desc

             from vw_apo_parent_child_dim

             where trim(cmp_id)::varchar = '7470'

             union all

             select distinct master_code,

                    parent_matl_desc

             from vw_apo_parent_child_dim

             where not (master_code in (select distinct master_code

                                        from vw_apo_parent_child_dim

                                        where trim(cmp_id)::varchar = '7470'))) mstrcd on vapcd.master_code = mstrcd.master_code)

         on eifs.co_cd = vapcd.cmp_id

        and eifs.matl_num = vapcd.matl_id

  left join (select materialnumber,

                    gcph_franchise,

                    gcph_brand,

                    gcph_subbrand,

                    gcph_variant,

                    gcph_needstate,

                    gcph_category,

                    gcph_subcategory,

                    gcph_segment,

                    gcph_subsegment

             from edw_gch_producthierarchy

             where ltrim(materialnumber,0) <> ''

             and   "region" = 'APAC') egph on ltrim (eifs.matl_num,0) = ltrim (egph.materialnumber,0)
) ,
non_open_orders as (select convert_timezone ('Australia/Sydney',doc_crt_dt)::date as snapshot_date,

       dct.jj_mnth_id as snapshot_date_jnj_month,

       orders.fisc_yr_src jj_mnth_id,

       orders.co_cd,

       orders.cust_num,

       orders.matl_num,

       orders.sls_doc,

       orders.curr_key,

       orders.rqst_delv_dt,

       rdt.jj_mnth_id as rqst_delv_dt_jnj_month,

       orders.gros_trd_sls

from (select eif.co_cd,

             eif.cust_num,

             eif.matl_num,

             eif.sls_doc,

             cast(eif.fisc_yr_src as numeric) as fisc_yr_src,

             eif.curr_key,

             eif.doc_crt_dt,

             rqst_delv_dt,

             sum(eif.gros_trd_sls) as gros_trd_sls

      from (select a.co_cd,

                   ltrim(a.cust_num,'0') as cust_num,

                   ltrim(a.matl_num,'0') as matl_num,

                   a.doc_crt_dt,

                   rqst_delv_dt,

                   a.gros_trd_sls,

                   ltrim(a.sls_doc,'0') as sls_doc,

                   substring(a.fisc_yr_src,1,4) ||substring(a.fisc_yr_src,6,2) as fisc_yr_src,

                   a.curr_key,

                   a.nts_bill,

                   a.fut_sls_qty

            from edw_invoice_fact a

            ) eif,

           (select distinct dly_sls_cust_attrb_lkp.cmp_id

            from dly_sls_cust_attrb_lkp) lkp

      where eif.co_cd = lkp.cmp_id

      group by eif.co_cd,

               eif.cust_num,

               eif.matl_num,

               eif.sls_doc,

               cast(eif.fisc_yr_src as numeric),

               eif.curr_key,

               eif.doc_crt_dt,

               rqst_delv_dt

              ) orders,

(select distinct cal_date,jj_mnth_id from edw_time_dim t1) rdt,

(select distinct cal_date,jj_mnth_id from edw_time_dim t1) dct

where  dct.cal_date=orders.doc_crt_dt

and rdt.cal_date=orders.rqst_delv_dt

) ,

final_non_open_orders as (

SELECT 'Non Open Orders' as subsource_type,        

        CASE

         WHEN EIFS.CO_CD = '7470' THEN 'Australia'

         WHEN EIFS.CO_CD = '8361' THEN 'New Zealand'

         ELSE NULL

       END AS COUNTRY,

       EIFS.SNAPSHOT_DATE,

       EIFS.SNAPSHOT_DATE_JNJ_MONTH,

       EIFS.JJ_MNTH_ID,

       ETD.JJ_MNTH,

       ETD.JJ_MNTH_SHRT AS JJ_MNTH_NM,

       ETD.JJ_YEAR,

       ETD.JJ_QRTR,

       LTRIM(EIFS.CUST_NUM) AS CUST_NUM,

       LTRIM(EIFS.MATL_NUM) AS MATL_NUM,

       EIFS.SLS_DOC,

       EIFS.RQST_DELV_DT,

       EIFS.RQST_DELV_DT_JNJ_MONTH,

       EBF.BILL_NUM,

       EBF.BILL_DT,

       EBF.BILL_DT_YYYY_MM,

       EBF.CREATED_ON,

       EMD.GRP_FRAN_DESC,

       EMD.PROD_FRAN_DESC,

       EMD.PROD_MJR_DESC,

       EMD.PROD_MNR_DESC,

       EMD.MATL_DESC,

       EMD.BRND_DESC,

       EGPH.GCPH_FRANCHISE,

       EGPH.GCPH_BRAND,

       EGPH.GCPH_SUBBRAND,

       EGPH.GCPH_VARIANT,

       EGPH.GCPH_NEEDSTATE,

       EGPH.GCPH_CATEGORY,

       EGPH.GCPH_SUBCATEGORY,

       EGPH.GCPH_SEGMENT,

       EGPH.GCPH_SUBSEGMENT,

       MSTRCD.MASTER_CODE,

       VCD.CHANNEL_DESC,

       VCD.SALES_OFFICE_DESC,

       VCD.CUST_NM,

       VCD.SALES_GRP_DESC,

       EIFS.CURR_KEY AS LOCAL_CCY,

       CURRENCY.TO_CCY AS TO_CCY,

       CURRENCY.EXCH_RATE AS EXCH_RATE,

       (EIFS.GROS_TRD_SLS*CURRENCY.EXCH_RATE) AS OPEN_ORDERS_VAL,

       (EBF.SUBTOTAL_1*CURRENCY.EXCH_RATE) AS GTS_LANDING_VAL

FROM NON_open_orders EIFS

  LEFT JOIN (SELECT DOC_NUM,

                    SOLD_TO,

                    MATERIAL,

                    BILL_NUM,

                    BILL_DT,

                    BDT.JJ_MNTH_ID BILL_DT_YYYY_MM,

                    CREATED_ON,

                    SUM(SUBTOTAL_1) AS SUBTOTAL_1

             FROM EDW_BILLING_FACT_SNAPSHOT EDW_BILLING_FACT JOIN (SELECT DISTINCT CAL_DATE,JJ_MNTH_ID FROM EDW_TIME_DIM T1) BDT ON BDT.CAL_DATE=EDW_BILLING_FACT.BILL_DT

             WHERE BILL_TYPE not in (select parameter_value from itg_query_parameters where country_code='ANZ' and parameter_name='Billing_Exclusion' and parameter_type='bill_type')

             AND (BILL_NUM IS NOT NULL AND BILL_DT IS NOT NULL)

             GROUP BY DOC_NUM,

                      SOLD_TO,

                      MATERIAL,

                      BILL_NUM,

                      BILL_DT,

                      BDT.JJ_MNTH_ID,

                      CREATED_ON

             HAVING COALESCE(SUM(SUBTOTAL_1),0)<>0) EBF

         ON LTRIM (EIFS.SLS_DOC,'0') = LTRIM (EBF.DOC_NUM,'0')

        AND LTRIM (EIFS.CUST_NUM,'0') = LTRIM (EBF.SOLD_TO,'0')

        AND LTRIM (EIFS.MATL_NUM,'0') = LTRIM (EBF.MATERIAL,'0')

  JOIN (SELECT DISTINCT JJ_MNTH,

               JJ_MNTH_SHRT,

               JJ_QRTR,

               JJ_YEAR,

               JJ_MNTH_ID

        FROM EDW_TIME_DIM) ETD ON EIFS.JJ_MNTH_ID = ETD.JJ_MNTH_ID

  INNER JOIN      (SELECT CAST(TO_CHAR(ADD_MONTHS (TO_DATE(T1.JJ_MNTH_ID::varchar,'YYYYMM'),- 1),'YYYYMM') AS INTEGER) AS JJ_PERIOD

                    FROM EDW_TIME_DIM T1

                    WHERE to_date(T1.CAL_DATE) = to_date(CONVERT_TIMEZONE('Australia/Sydney',current_timestamp()))

                    UNION

                    SELECT T1.JJ_MNTH_ID AS JJ_PERIOD

                    FROM EDW_TIME_DIM T1

                    WHERE to_date(T1.CAL_DATE) = to_date(CONVERT_TIMEZONE('Australia/Sydney',current_timestamp()))) ETD1 ON ETD1.JJ_PERIOD :: varchar(10)=EBF.BILL_DT_YYYY_MM

  JOIN (SELECT RATE_TYPE,

               FROM_CCY,

               TO_CCY,

               JJ_MNTH_ID,

               EXCH_RATE

        FROM VW_JJBR_CURR_EXCH_DIM

        WHERE EXCH_RATE = 1

        AND   FROM_CCY = 'AUD'

        UNION ALL

        SELECT RATE_TYPE,

               FROM_CCY,

               TO_CCY,

               JJ_MNTH_ID,

               EXCH_RATE

        FROM VW_BWAR_CURR_EXCH_DIM) CURRENCY

    ON EIFS.CURR_KEY = CURRENCY.FROM_CCY

   AND ETD.JJ_MNTH_ID = CURRENCY.JJ_MNTH_ID

  LEFT JOIN VW_CUSTOMER_DIM VCD ON LTRIM (EIFS.CUST_NUM,'0') = LTRIM (VCD.CUST_NO,'0')

  LEFT JOIN EDW_MATERIAL_DIM EMD ON LTRIM (EIFS.MATL_NUM,'0') = LTRIM (EMD.MATL_ID,'0')

  LEFT JOIN (VW_APO_PARENT_CHILD_DIM VAPCD

  LEFT JOIN (SELECT DISTINCT MASTER_CODE,

                    PARENT_MATL_DESC

             FROM VW_APO_PARENT_CHILD_DIM

             WHERE trim(CMP_ID)::varchar = '7470'

             UNION ALL

             SELECT DISTINCT MASTER_CODE,

                    PARENT_MATL_DESC

             FROM VW_APO_PARENT_CHILD_DIM

             WHERE NOT (MASTER_CODE IN (SELECT DISTINCT MASTER_CODE

                                        FROM VW_APO_PARENT_CHILD_DIM

                                        WHERE trim(CMP_ID)::varchar = '7470'))) MSTRCD ON VAPCD.MASTER_CODE = MSTRCD.MASTER_CODE)

         ON EIFS.CO_CD = VAPCD.CMP_ID

        AND EIFS.MATL_NUM = VAPCD.MATL_ID

  LEFT JOIN (SELECT MATERIALNUMBER,

                    GCPH_FRANCHISE,

                    GCPH_BRAND,

                    GCPH_SUBBRAND,

                    GCPH_VARIANT,

                    GCPH_NEEDSTATE,

                    GCPH_CATEGORY,

                    GCPH_SUBCATEGORY,

                    GCPH_SEGMENT,

                    GCPH_SUBSEGMENT

             FROM EDW_GCH_PRODUCTHIERARCHY

             WHERE LTRIM(MATERIALNUMBER,0) <> ''

             AND   "region" = 'APAC') EGPH ON LTRIM (EIFS.MATL_NUM,0) = LTRIM (EGPH.MATERIALNUMBER,0)

  JOIN EDW_SAPBW_PLAN_LKP ESPL ON ESPL.SLS_GRP_CD = VCD.SALES_GRP_CD

  WHERE  (BILL_NUM IS NOT NULL AND BILL_DT IS NOT NULL)

)
,
open_orders as (
select
'open orders' as subsource_type,

  country,

  snapshot_date,

  999999 :: numeric as snapshot_date_jnj_month,

  jj_mnth_id,

  jj_mnth,

  jj_mnth_nm,

  jj_year,

  jj_qrtr,

  cust_num,

  matl_num,

  sls_doc,

  '9/9/9999':: date as rqst_delv_dt,

  999999 :: numeric as rqst_delv_dt_jnj_month,

  bill_num,

  bill_dt,

  999999 :: numeric as bill_dt_yyyy_mm,

  created_on,

  grp_fran_desc,

  prod_fran_desc,

  prod_mjr_desc,

  prod_mnr_desc,

  matl_desc,

  brnd_desc,

  gcph_franchise,

  gcph_brand,

  gcph_subbrand,

  gcph_variant,

  gcph_needstate,

  gcph_category,

  gcph_subcategory,

  gcph_segment,

  gcph_subsegment,

  master_code,

  channel_desc,

  sales_office_desc,

  cust_nm,

  sales_grp_desc,

  local_ccy,

  to_ccy,

  exch_rate,

  open_orders_val,

  gts_landing_val

  from WKS_GTS_VISIBILITY
),
transformed as (
select * from final_non_open_orders
union all
select * from open_orders
) ,
final as (
select
subsource_type::varchar(15) as subsource_type,
country::varchar(11) as country,
snapshot_date::date as snapshot_date,
snapshot_date_jnj_month::number(18,0) as snapshot_date_jnj_month,
jj_mnth_id::number(18,0) as jj_mnth_id,
jj_mnth::number(18,0) as jj_mnth,
jj_mnth_nm::varchar(3) as jj_mnth_nm,
jj_year::number(18,0) as jj_year,
jj_qrtr::number(18,0) as jj_qrtr,
cust_num::varchar(10) as cust_num,
matl_num::varchar(18) as matl_num,
sls_doc::varchar(10) as sls_doc,
rqst_delv_dt::date as rqst_delv_dt,
rqst_delv_dt_jnj_month::number(18,0) as rqst_delv_dt_jnj_month,
bill_num::varchar(50) as bill_num,
bill_dt::date as bill_dt,
bill_dt_yyyy_mm::number(18,0) as bill_dt_yyyy_mm,
created_on::date as created_on,
grp_fran_desc::varchar(100) as grp_fran_desc,
prod_fran_desc::varchar(100) as prod_fran_desc,
prod_mjr_desc::varchar(100) as prod_mjr_desc,
prod_mnr_desc::varchar(100) as prod_mnr_desc,
matl_desc::varchar(100) as matl_desc,
brnd_desc::varchar(100) as brnd_desc,
gcph_franchise::varchar(30) as gcph_franchise,
gcph_brand::varchar(30) as gcph_brand,
gcph_subbrand::varchar(100) as gcph_subbrand,
gcph_variant::varchar(100) as gcph_variant,
gcph_needstate::varchar(50) as gcph_needstate,
gcph_category::varchar(50) as gcph_category,
gcph_subcategory::varchar(50) as gcph_subcategory,
gcph_segment::varchar(50) as gcph_segment,
gcph_subsegment::varchar(100) as gcph_subsegment,
master_code::varchar(18) as master_code,
channel_desc::varchar(20) as channel_desc,
sales_office_desc::varchar(30) as sales_office_desc,
cust_nm::varchar(100) as cust_nm,
sales_grp_desc::varchar(30) as sales_grp_desc,
local_ccy::varchar(5) as local_ccy,
to_ccy::varchar(5) as to_ccy,
exch_rate::number(15,5) as exch_rate,
open_orders_val::number(38,9) as open_orders_val,
gts_landing_val::number(38,9) as gts_landing_val
from transformed)
select * from final