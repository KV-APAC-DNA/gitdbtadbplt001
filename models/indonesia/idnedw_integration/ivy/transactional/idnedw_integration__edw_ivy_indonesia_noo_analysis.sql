with vw_all_distributor_sellout_sales_fact as (
    select * from {{ ref('idnedw_integration__vw_all_distributor_sellout_sales_fact') }}
),
vw_all_distributor_ivy_sellout_sales_fact as (
    select * from {{ ref('idnedw_integration__vw_all_distributor_ivy_sellout_sales_fact') }}
),
edw_distributor_dim as (
    select * from  {{ ref('idnedw_integration__edw_distributor_dim') }}
),
edw_product_dim as (
    select * from {{ ref('idnedw_integration__edw_product_dim') }}
),
edw_distributor_customer_dim as (
    select * from {{ ref('idnedw_integration__edw_distributor_customer_dim') }}
),
edw_distributor_salesman_dim as (
    select * from {{ ref('idnedw_integration__edw_distributor_salesman_dim') }}
),
edw_time_dim as (
    select * from {{ source('idnedw_integration', 'edw_time_dim') }}
),
edw_distributor_ivy_outlet_master as (
    select * from {{ ref('idnedw_integration__edw_distributor_ivy_outlet_master') }}
),
itg_target_dist_brand_channel as (
    select * from  {{ ref('idnitg_integration__itg_target_dist_brand_channel') }}
),
vw_all_distributor_sellout_sales_fact_grouped as (
    select 
        trans_key,
        bill_doc,
        bill_dt,
        null as order_id,
        jj_mnth_id,
        jj_wk,
        dstrbtr_grp_cd,
        dstrbtr_id,
        jj_sap_dstrbtr_id,
        dstrbtr_cust_id,
        dstrbtr_prod_id,
        jj_sap_prod_id,
        dstrbtn_chnl,
        grp_outlet,
        dstrbtr_slsmn_id,
        sum(sls_qty) as sls_qty,
        sum(grs_val) as grs_val,
        sum(jj_net_val) as jj_net_val,
        sum(trd_dscnt) as trd_dscnt,
        sum(dstrbtr_net_val) as dstrbtr_net_val,
        sum(rtrn_qty) as rtrn_qty,
        sum(rtrn_val) as rtrn_val 
    from vw_all_distributor_sellout_sales_fact
       where (
            dstrbtr_grp_cd not in ('DNR','CSA','PON','SAS','RFS','SNC','DSD','AWS','GMP','SPS','PMJ')
	        and 
            (dstrbtr_grp_cd <> 'ADT' or jj_sap_dstrbtr_id <> '117089')
        )
       group by
            trans_key,
            bill_doc,
            bill_dt,
            jj_mnth_id,
            jj_wk,
            dstrbtr_grp_cd,
            dstrbtr_id,
            jj_sap_dstrbtr_id,
            dstrbtr_cust_id,
            dstrbtr_prod_id,
            jj_sap_prod_id,
            dstrbtn_chnl,
            grp_outlet,
            dstrbtr_slsmn_id
       having sum(jj_net_val) <> 0
),
vw_all_distributor_ivy_sellout_sales_fact_grouped as (
    select 
        trans_key,
        bill_doc,
        bill_dt,
        order_id,
        jj_mnth_id,
        jj_wk,
        dstrbtr_grp_cd,
        dstrbtr_id,
        jj_sap_dstrbtr_id,
        dstrbtr_cust_id,
        dstrbtr_prod_id,
        jj_sap_prod_id,
        dstrbtn_chnl,
        grp_outlet,
        dstrbtr_slsmn_id,
        sum(sls_qty) as sls_qty,
        sum(grs_val) as grs_val,
        sum(jj_net_val) as jj_net_val,
        sum(trd_dscnt) as trd_dscnt,
        sum(dstrbtr_net_val) as dstrbtr_net_val,
        sum(rtrn_qty) as rtrn_qty,
        sum(rtrn_val) as rtrn_val 
    from  vw_all_distributor_ivy_sellout_sales_fact
    where (dstrbtr_grp_cd  in ('DNR','CSA','PON','SAS','RFS','SNC','DSD','AWS','GMP','SPS','PMJ') 
        or (dstrbtr_grp_cd = 'ADT' and jj_sap_dstrbtr_id = '117089'))
    GROUP BY
        trans_key,
        bill_doc,
        bill_dt,
        order_id,
        jj_mnth_id,
        jj_wk,
        dstrbtr_grp_cd,
        dstrbtr_id,
        jj_sap_dstrbtr_id,
        dstrbtr_cust_id,
        dstrbtr_prod_id,
        jj_sap_prod_id,
        dstrbtn_chnl,
        grp_outlet,
        dstrbtr_slsmn_id
    having SUM(jj_net_val) <> 0
),
final_transformed_1 as (
    select 
        etd.jj_year,
        etd.jj_qrtr,
        etd.jj_mnth,
        etd.jj_wk,
        etd.jj_mnth_wk_no,
        etd.jj_mnth_no,
        eadssf.bill_doc,
        eadssf.bill_dt,
        eadssf.order_id,
        trim(upper(edd.dstrbtr_grp_cd)) as dstrbtr_grp_cd,
        edd.dstrbtr_grp_nm,
        trim(upper(edd.jj_sap_dstrbtr_id)) as jj_sap_dstrbtr_id,
        edd.jj_sap_dstrbtr_nm,
        edd.jj_sap_dstrbtr_nm || ' ^' || edd.jj_sap_dstrbtr_id as dstrbtr_cd_nm,
        edd.area,
        edd.region,
        edd.bdm_nm,
        edd.rbm_nm,
        edd.status as dstrbtr_status,
        trim(upper(edcd.cust_id_map)) as cust_id_map,
        nvl(edcd.cust_nm_map,'') as cust_nm_map,
        (nvl(edcd.cust_nm_map,'')  || ' ^' || edcd.cust_id_map || ' ^' || edcd.jjid) as dstrbtr_cust_cd_nm,
        edcd.cust_grp,
        trim(upper(edcd.chnl)) as chnl,
        trim(upper(edcd.outlet_type)) as outlet_type,
        edcd.chnl_grp,
        edcd.jjid,
        edcd.chnl_grp2,
        upper(edd.prov_nm || ' - ' || edcd.city) as city,
        (case when date(edcd.cust_crtd_dt) < '2015-12-29' then 'EXISTING' when date(edcd.cust_crtd_dt) between '2016-01-04' and '2016-12-31' then 'NOO 2016' else 'NOO 2017' end) as cust_status,
        trim(upper(epd.jj_sap_prod_id)) as jj_sap_prod_id,
        epd.jj_sap_prod_desc,
        epd.jj_sap_upgrd_prod_id,
        epd.jj_sap_upgrd_prod_desc,
        epd.jj_sap_cd_mp_prod_id,
        epd.jj_sap_cd_mp_prod_desc,
        epd.jj_sap_upgrd_prod_desc || ' ^' || epd.jj_sap_upgrd_prod_id as sap_prod_code_name,
        epd.franchise,
        epd.brand,
        epd.variant1,
        epd.variant2,
        epd.variant3 as variant,
        epd.variant3 || ' ' || nvl(cast(epd.put_up as varchar),'') as put_up,
        epd.status as prod_status,
        nvl(trim(upper(edsd.slsmn_id)),'Noname') as slsmn_id,
        nvl(edsd.slsmn_nm,'Noname') as slsmn_nm,
        eadssf.sls_qty,
        eadssf.grs_val as hna,
        eadssf.jj_net_val as niv,
        eadssf.trd_dscnt,
        eadssf.dstrbtr_net_val as dstrbtr_niv,
        eadssf.rtrn_qty,
        eadssf.rtrn_val,
        0 as hsku_target_growth,
        0 as hsku_target_coverage,
        etd.jj_mnth_long as jj_mnth_long,
        0 as trgt_hna,
        0 as trgt_niv,
        null as npi_flag,
        null as benchmark_sku_code,
        null as sku_benchmark,  
        null as hero_sku_flag,
        null as trgt_dist_brnd_chnl_flag,
        edcd.cust_grp2 as tiering
    from  vw_all_distributor_sellout_sales_fact_grouped as eadssf,
        edw_distributor_dim as edd,
        edw_product_dim as epd,
        edw_distributor_customer_dim as edcd,
        edw_distributor_salesman_dim as edsd,
     (select 
        distinct 
            jj_year,
            jj_qrtr_no,
            jj_qrtr,
            jj_mnth_id,
            jj_mnth,
            jj_mnth_no,
            jj_mnth_shrt,
            jj_mnth_long,
            jj_wk,
            jj_mnth_wk_no,
            jj_date,
            cal_date
      from edw_time_dim) as etd
where  
    trim(upper(edd.jj_sap_dstrbtr_id(+)))::text = trim(upper(eadssf.jj_sap_dstrbtr_id))::text
    and   eadssf.jj_mnth_id between edd.effective_from(+) and edd.effective_to(+) 
    and   trim(upper(edcd.jj_sap_dstrbtr_id(+)))::text = trim(upper(eadssf.jj_sap_dstrbtr_id))::text
    and   trim(upper(edcd.cust_id(+)))::text = trim(upper(eadssf.dstrbtr_cust_id))::text
    and   eadssf.jj_mnth_id between edcd.effective_from(+) and edcd.effective_to(+)
    and   date(etd.cal_date) = date(eadssf.bill_dt)
    and   trim(upper(epd.jj_sap_prod_id(+)))::text = trim(upper(eadssf.jj_sap_prod_id))::text
    and   eadssf.jj_mnth_id between epd.effective_from(+) and epd.effective_to(+) 
    and   trim(upper(edsd.slsmn_id(+)))::text = trim(upper(eadssf.dstrbtr_slsmn_id))::text
    and   trim(upper(edsd.jj_sap_dstrbtr_id(+)))::text = trim(upper(eadssf.jj_sap_dstrbtr_id))::text 
    and   eadssf.jj_mnth_id between edsd.effective_from(+) and edsd.effective_to(+) 
    and   (not (trim(upper(edd.dstrbtr_grp_cd))::text in ('PZI')))
    and   trim(upper(eadssf.dstrbtr_grp_cd))::text !='SPLD'
    and   trim(upper(eadssf.dstrbtr_grp_cd))::text !='JYM'
    and   trim(upper(eadssf.jj_sap_prod_id))::text is not null 
    and   trim(upper(eadssf.jj_sap_prod_id))::text !='DAOG20'
),
final_transformed_2 as (
    SELECT 
        etd.jj_year,
        etd.jj_qrtr,
        etd.jj_mnth,
        etd.jj_wk,
        etd.jj_mnth_wk_no,
        etd.jj_mnth_no,
        eadssf.bill_doc,
        eadssf.bill_dt,
        eadssf.order_id,
        trim(upper(edd.dstrbtr_grp_cd)) as dstrbtr_grp_cd,
        edd.dstrbtr_grp_nm,
        trim(upper(edd.jj_sap_dstrbtr_id)) as jj_sap_dstrbtr_id,
        edd.jj_sap_dstrbtr_nm,
        edd.jj_sap_dstrbtr_nm || ' ^' || edd.jj_sap_dstrbtr_id as dstrbtr_cd_nm,
        edd.area,
        edd.region,
        edd.bdm_nm,
        edd.rbm_nm,
        edd.status as dstrbtr_status,
        trim(upper(edcd.cust_id_map)) as cust_id_map,
        nvl(edcd.cust_nm_map,'') as cust_nm_map,
        (nvl(edcd.cust_nm_map,'') || ' ^' || edcd.cust_id_map || ' ^' || edcd.jjid) as dstrbtr_cust_cd_nm,
        edcd.cust_grp,
        trim(upper(edcd.chnl)) as chnl,
        trim(upper(edcd.outlet_type)) as outlet_type,
        edcd.chnl_grp,
        edcd.jjid,
        edcd.chnl_grp2,
        upper(edd.prov_nm || ' - ' || edcd.city) as city,
        (case when date(edcd.cust_crtd_dt) < '2015-12-29' then 'EXISTING' when date(edcd.cust_crtd_dt) between '2016-01-04' and '2016-12-31' then 'NOO 2016' else 'NOO 2017' end) as cust_status,
        trim(upper(epd.jj_sap_prod_id)) as jj_sap_prod_id,
        epd.jj_sap_prod_desc,
        epd.jj_sap_upgrd_prod_id,
        epd.jj_sap_upgrd_prod_desc,
        epd.jj_sap_cd_mp_prod_id,
        epd.jj_sap_cd_mp_prod_desc,
        epd.jj_sap_upgrd_prod_desc || ' ^' || epd.jj_sap_upgrd_prod_id as sap_prod_code_name,
        epd.franchise,
        epd.brand,
        epd.variant1,
        epd.variant2,
        epd.variant3 as variant,
        epd.variant3 || ' ' || nvl(cast(epd.put_up as varchar),'') as put_up,
        epd.status as prod_status,
        nvl(trim(upper(edsd.slsmn_id)),'Noname') as slsmn_id,
        nvl(edsd.slsmn_nm,'Noname') as slsmn_nm,
        eadssf.sls_qty,
        eadssf.grs_val as hna,
        eadssf.jj_net_val as niv,
        eadssf.trd_dscnt,
        eadssf.dstrbtr_net_val as dstrbtr_niv,
        eadssf.rtrn_qty,
        eadssf.rtrn_val,
        0 as hsku_target_growth,
        0 as hsku_target_coverage,
        etd.jj_mnth_long as jj_mnth_long,
        0 as trgt_hna,
        0 as trgt_niv,
        null as npi_flag,
        null as benchmark_sku_code,
        null as sku_benchmark,  
        null as hero_sku_flag,
        null as trgt_dist_brnd_chnl_flag,
        edcd.cust_grp2 as tiering
       from vw_all_distributor_ivy_sellout_sales_fact_grouped as eadssf,
        edw_distributor_dim as edd,
        edw_product_dim as epd,
        edw_distributor_ivy_outlet_master as edcd,
        edw_distributor_salesman_dim as edsd,
     (select distinct jj_year,
             jj_qrtr_no,
             jj_qrtr,
             jj_mnth_id,
             jj_mnth,
             jj_mnth_no,
             jj_mnth_shrt,
             jj_mnth_long,
             jj_wk,
             jj_mnth_wk_no,
             jj_date,
             cal_date
      from edw_time_dim) as etd
where   
    trim(upper(edd.jj_sap_dstrbtr_id(+))) = trim(upper(eadssf.jj_sap_dstrbtr_id))
    and   eadssf.jj_mnth_id between edd.effective_from(+) and edd.effective_to(+) 
    and   trim(upper(edcd.jj_sap_dstrbtr_id(+))) = trim(upper(eadssf.jj_sap_dstrbtr_id))
    and   trim(upper(edcd.cust_id(+))) = trim(upper(eadssf.dstrbtr_cust_id))
    and   date(etd.cal_date) = date(eadssf.bill_dt)
    and   trim(upper(epd.jj_sap_prod_id(+))) = trim(upper(eadssf.jj_sap_prod_id))
    and   eadssf.jj_mnth_id between epd.effective_from(+) and epd.effective_to(+) 
    and   trim(upper(edsd.slsmn_id(+))) = trim(upper(eadssf.dstrbtr_slsmn_id))
    and   trim(upper(edsd.jj_sap_dstrbtr_id(+))) = trim(upper(eadssf.jj_sap_dstrbtr_id)) 
    and   eadssf.jj_mnth_id between edsd.effective_from(+) and edsd.effective_to(+) 
    and   (not (trim(upper(edd.dstrbtr_grp_cd)) in ('PZI')))
    and   trim(upper(eadssf.dstrbtr_grp_cd))!='SPLD'
    and   trim(upper(eadssf.dstrbtr_grp_cd))!='JYM'
    and   trim(upper(eadssf.jj_sap_prod_id)) is not null 
    and   trim(upper(eadssf.jj_sap_prod_id))!='DAOG20'
),
final_transformed_3 as (
    SELECT 
        year AS JJ_YEAR,
        etd.jj_qrtr as jj_qrtr,
        etd.jj_mnth as jj_mnth,
        0 as jj_wk,
        0 as jj_mnth_wk_no,
        etd.jj_mnth_no as jj_mnth_no,
        null as bill_doc,
        null as bill_dt,
        null as order_id,
        null as dstrbtr_grp_cd,
        null as dstrbtr_grp_nm,
        trim(upper(h.jj_sap_dstrbtr_id)) as jj_sap_dstrbtr_id,
        h.jj_sap_dstrbtr_nm as jj_sap_dstrbtr_nm,
        h.jj_sap_dstrbtr_nm || ' ^' || h.jj_sap_dstrbtr_id as dstrbtr_cd_nm,
        d.area as area,
        d.region as region,
        d.bdm_nm as bdm_nm,
        d.rbm_nm as rbm_nm,
        d.status as dstrbtr_status,
        null as cust_id_map,
        null as cust_nm_map,
        null as dstrbtr_cust_cd_nm,
        null as cust_grp,
        h.channel as chnl,
        null as outlet_type,
        null as chnl_grp,
        null as jjid,
        null as chnl_grp2,
        null as city,
        null as cust_status,
        null as jj_sap_prod_id,
        null as jj_sap_prod_desc,
        null as jj_sap_upgrd_prod_id,
        null as jj_sap_upgrd_prod_desc,
        null as jj_sap_cd_mp_prod_id,
        null as jj_sap_cd_mp_prod_desc,
        null as sap_prod_code_name,
        case
                when h.brand is not null then epd.franchise
                else h.franchise
        end as franchise,
        h.brand as brand,
        null as variant1,
        null as variant2,
        null as variant,
        null as put_up,
        null as prod_status,
        null as slsmn_id,
        null as slsmn_nm,
        0 as sls_qty,
        0 as hna,
        0 as niv,
        0 as trd_dscnt,
        0 as dstrbtr_niv,
        0 as rtrn_qty,
        0 as rtrn_val,
        0 as hsku_target_growth,
        0 as hsku_target_coverage,
        h.jj_mnth_long as jj_mnth_long,
        h.trgt_hna as trgt_hna,
        h.trgt_niv as trgt_niv,
        null as npi_flag,
        null as benchmark_sku_code,
        null as sku_benchmark,
        null as hero_sku_flag,
        'Y' as trgt_dist_brnd_chnl_flag,
        null as tiering
    from itg_target_dist_brand_channel h
    left join (select distinct jj_year,
                        jj_qrtr_no,
                        jj_qrtr,
                        jj_mnth_id,
                        jj_mnth,
                        jj_mnth_no,
                        jj_mnth_shrt,
                        jj_mnth_long
                from edw_time_dim) as etd
            on h.year = etd.jj_year
            AND UPPER (TRIM (h.jj_mnth_long)) = UPPER (TRIM (ETD.JJ_MNTH_LONG))
    LEFT JOIN edw_distributor_dim d ON 
        TRIM (UPPER (h.jj_sap_dstrbtr_id)) = TRIM (UPPER (d.jj_sap_dstrbtr_id))
    and 
        concat(h.year,decode(upper(trim(h.jj_mnth_long)),'JANUARY','01','FEBRUARY','02','MARCH','03','APRIL','04','MAY','05','JUNE','06','JULY','07','AUGUST','08',
    'SEPTEMBER','09','OCTOBER','10','NOVEMBER','11','DECEMBER','12','00')) between d.effective_from and d.effective_to 
    left join (select distinct brand, franchise,effective_from,effective_to from edw_product_dim) epd
            on case when h.brand is not null
            and upper (trim (h.brand)) = upper (trim (epd.brand)) 
            and concat(h.year,decode(upper(trim(h.jj_mnth_long)),'JANUARY','01','FEBRUARY','02','MARCH','03','APRIL','04','MAY','05','JUNE','06','JULY','07','AUGUST','08',
    'SEPTEMBER','09','OCTOBER','10','NOVEMBER','11','DECEMBER','12','00')) between epd.effective_from and epd.effective_to
    then 1 end = 1
)
,
unioned as (
    select * from final_transformed_3
    union all 
    select * from final_transformed_2
    union all 
    select * from final_transformed_1
),
final as (
    select 
        jj_year::number(18,0) as jj_year,
        jj_qrtr::varchar(24) as jj_qrtr,
        jj_mnth::varchar(25) as jj_mnth,
        jj_wk::number(18,0) as jj_wk,
        jj_mnth_wk_no::number(38,0) as jj_mnth_wk_no,
        jj_mnth_no::number(18,0) as jj_mnth_no,
        bill_doc::varchar(100) as bill_doc,
        bill_dt::timestamp_ntz(9) as bill_dt,
        order_id::varchar(50) as order_id,
        dstrbtr_grp_cd::varchar(25) as dstrbtr_grp_cd,
        dstrbtr_grp_nm::varchar(155) as dstrbtr_grp_nm,
        jj_sap_dstrbtr_id::varchar(75) as jj_sap_dstrbtr_id,
        jj_sap_dstrbtr_nm::varchar(75) as jj_sap_dstrbtr_nm,
        dstrbtr_cd_nm::varchar(152) as dstrbtr_cd_nm,
        area::varchar(50) as area,
        region::varchar(50) as region,
        bdm_nm::varchar(50) as bdm_nm,
        rbm_nm::varchar(50) as rbm_nm,
        dstrbtr_status::varchar(50) as dstrbtr_status,
        cust_id_map::varchar(100) as cust_id_map,
        cust_nm_map::varchar(100) as cust_nm_map,
        dstrbtr_cust_cd_nm::varchar(304) as dstrbtr_cust_cd_nm,
        cust_grp::varchar(100) as cust_grp,
        chnl::varchar(100) as chnl,
        outlet_type::varchar(100) as outlet_type,
        chnl_grp::varchar(100) as chnl_grp,
        jjid::varchar(100) as jjid,
        chnl_grp2::varchar(100) as chnl_grp2,
        city::varchar(229) as city,
        cust_status::varchar(8) as cust_status,
        jj_sap_prod_id::varchar(50) as jj_sap_prod_id,
        jj_sap_prod_desc::varchar(100) as jj_sap_prod_desc,
        jj_sap_upgrd_prod_id::varchar(50) as jj_sap_upgrd_prod_id,
        jj_sap_upgrd_prod_desc::varchar(100) as jj_sap_upgrd_prod_desc,
        jj_sap_cd_mp_prod_id::varchar(50) as jj_sap_cd_mp_prod_id,
        jj_sap_cd_mp_prod_desc::varchar(100) as jj_sap_cd_mp_prod_desc,
        sap_prod_code_name::varchar(152) as sap_prod_code_name,
        franchise::varchar(50) as franchise,
        brand::varchar(50) as brand,
        variant1::varchar(50) as variant1,
        variant2::varchar(50) as variant2,
        variant::varchar(50) as variant,
        put_up::varchar(62) as put_up,
        prod_status::varchar(50) as prod_status,
        slsmn_id::varchar(100) as slsmn_id,
        slsmn_nm::varchar(100) as slsmn_nm,
        sls_qty::number(18,4) as sls_qty,
        hna::number(18,4) as hna,
        niv::number(18,4) as niv,
        trd_dscnt::number(18,4) as trd_dscnt,
        dstrbtr_niv::number(18,4) as dstrbtr_niv,
        rtrn_qty::number(18,4) as rtrn_qty,
        rtrn_val::number(18,4) as rtrn_val,
        hsku_target_growth::number(18,4) as hsku_target_growth,
        hsku_target_coverage::number(18,4) as hsku_target_coverage,
        jj_mnth_long::varchar(10) as jj_mnth_long,
        trgt_hna::number(38,4) as trgt_hna,
        trgt_niv::number(38,4) as trgt_niv,
        npi_flag::varchar(1) as npi_flag,
        benchmark_sku_code::varchar(75) as benchmark_sku_code,
        sku_benchmark::varchar(75) as sku_benchmark,
        hero_sku_flag::varchar(1) as hero_sku_flag,
        trgt_dist_brnd_chnl_flag::varchar(1) as trgt_dist_brnd_chnl_flag,
        tiering::varchar(100) as tiering
    from unioned
)
select * from final