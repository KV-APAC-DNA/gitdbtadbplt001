with edw_vw_id_sellin_fact as (
    select * from {{ ref('idnedw_integration__edw_vw_id_sellin_fact') }}
),
edw_id_sellin_fact_temp as (
   select rec_key,
       cust_prod_cd,
       bill_dt,
       bill_doc,
       jj_mnth_id,
       jj_sap_dstrbtr_id,
       jj_sap_prod_id,
       sellin_qty,
       sellin_val,
       gross_sellin_val
    from edw_vw_id_sellin_fact
),
edw_distributor_dim as (
    select * from {{ ref('idnedw_integration__edw_distributor_dim') }}
),
edw_product_dim as (
    select * from {{ ref('idnedw_integration__edw_product_dim') }}
),
etd as (
    select distinct jj_year,
        jj_qrtr_no,
        jj_qrtr,
        jj_mnth_id,
        jj_mnth,
        jj_mnth_no,
        jj_mnth_desc,
        jj_mnth_shrt,
        jj_mnth_long,
        jj_wk,
        jj_mnth_wk_no,
        jj_date,
        cal_date
 from {{ source('idnedw_integration', 'edw_time_dim') }}
),
itg_query_parameters as (
    select * from {{ source('aspitg_integration', 'itg_query_parameters') }}
),
final as (
select to_date(eadlf.bill_dt) as bill_dt,
       eadlf.bill_doc::varchar(100) as bill_doc,
       etd.jj_year::number(18,0) as jj_year,
       etd.jj_qrtr::varchar(24) as jj_qrtr,
       etd.jj_mnth_id::number(18,0) as jj_mnth_id,
       etd.jj_mnth::varchar(25) as jj_mnth,
       etd.jj_mnth_desc::varchar(20) as jj_mnth_desc,
       etd.jj_mnth_no::number(18,0) as jj_mnth_no,
       etd.jj_mnth_long::varchar(75) as jj_mnth_long,
       edd.dstrbtr_grp_cd::varchar(25) as dstrbtr_grp_cd,
       edd.dstrbtr_grp_nm::varchar(155) as dstrbtr_grp_nm,
       edd.jj_sap_dstrbtr_id::varchar(75) as jj_sap_dstrbtr_id,
       edd.jj_sap_dstrbtr_nm::varchar(75) as jj_sap_dstrbtr_nm,
       (edd.jj_sap_dstrbtr_nm || ' ^' || edd.jj_sap_dstrbtr_id)::VARCHAR(72) as dstrbtr_cd_nm,
       edd.area::VARCHAR(50) as area,
       edd.region::VARCHAR(50) as region,
       edd.bdm_nm::VARCHAR(50) as bdm_nm,
       edd.rbm_nm::VARCHAR(50) as rbm_nm,
       edd.status::VARCHAR(50) as dstrbtr_status,
       epd.jj_sap_prod_id::VARCHAR(50) as jj_sap_prod_id,
       epd.jj_sap_prod_desc::VARCHAR(100) as jj_sap_prod_desc,
       epd.jj_sap_upgrd_prod_id::VARCHAR(50) as jj_sap_upgrd_prod_id,
       epd.jj_sap_upgrd_prod_desc::VARCHAR(100) as jj_sap_upgrd_prod_desc,
       epd.jj_sap_cd_mp_prod_id::VARCHAR(50) as jj_sap_cd_mp_prod_id,
       epd.jj_sap_cd_mp_prod_desc::VARCHAR(100) as jj_sap_cd_mp_prod_desc,
       (epd.jj_sap_upgrd_prod_desc || ' ^' || epd.jj_sap_upgrd_prod_id)::VARCHAR(152) as sap_prod_code_name,
       epd.franchise::VARCHAR(75) as franchise,
       epd.brand::VARCHAR(75) as brand,
       epd.variant1::VARCHAR(50) as sku_grp_or_variant,
       epd.variant2::VARCHAR(50) as sku_grp1_or_variant1,
       epd.variant3::VARCHAR(50) as sku_grp2_or_variant2,
       (epd.variant3 || ' ' || nvl(cast(epd.put_up as varchar),''))::VARCHAR(62) as sku_grp3_or_variant3,
       epd.status::VARCHAR(50) as prod_status,
       sum(eadlf.sellin_qty)::NUMBER(18,4) as sellin_qty,
       sum(eadlf.sellin_val)::NUMBER(18,4) as sellin_val,
       sum(eadlf.gross_sellin_val)::NUMBER(18,4) as gross_sellin_val
from edw_id_sellin_fact_temp as eadlf,
     edw_distributor_dim as edd,
     edw_product_dim as epd,
     etd
where
trim(edd.jj_sap_dstrbtr_id(+)) = trim(eadlf.jj_sap_dstrbtr_id)
and eadlf.jj_mnth_id between edd.effective_from(+) and edd.effective_to(+)     
and   date(etd.cal_date) = date(eadlf.bill_dt)
and   trim(epd.jj_sap_prod_id(+)) = trim(eadlf.jj_sap_prod_id) 
and eadlf.jj_mnth_id between epd.effective_from(+) and epd.effective_to(+)  
and   etd.jj_year >= date_part(year,convert_timezone('UTC',current_timestamp))- (select cast(parameter_value as int)
                                                  from itg_query_parameters
                                                  where upper(country_code) = 'ID'
                                                  and   upper(parameter_name) = 'SELLIN_ANALYSIS_TDE_DATA_RETENTION_YEARS')
group by eadlf.bill_dt,
         eadlf.bill_doc,
         etd.jj_year,
         etd.jj_qrtr,
         etd.jj_mnth_id,
         etd.jj_mnth,
         etd.jj_mnth_desc,
         etd.jj_mnth_no,
         etd.jj_mnth_long,
         edd.dstrbtr_grp_cd,
         edd.dstrbtr_grp_nm,
         edd.jj_sap_dstrbtr_id,
         edd.jj_sap_dstrbtr_nm,
         (edd.jj_sap_dstrbtr_nm || ' ^' || edd.jj_sap_dstrbtr_id),
         edd.area,
         edd.region,
         edd.bdm_nm,
         edd.rbm_nm,
         edd.status,
         epd.jj_sap_prod_id,
         epd.jj_sap_prod_desc,
         epd.jj_sap_upgrd_prod_id,
         epd.jj_sap_upgrd_prod_desc,
         epd.jj_sap_cd_mp_prod_id,
         epd.jj_sap_cd_mp_prod_desc,
         epd.jj_sap_upgrd_prod_desc || ' ^' || epd.jj_sap_upgrd_prod_id,
         epd.franchise,
         epd.brand,
         epd.variant1,
         epd.variant2,
         epd.variant3,
         epd.variant3 || ' ' || nvl(cast(epd.put_up as varchar),''),
         epd.status
)
select * from final