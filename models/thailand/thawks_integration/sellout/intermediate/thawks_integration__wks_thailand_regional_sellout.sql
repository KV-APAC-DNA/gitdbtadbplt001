with wks_thailand_regional_sellout_base as (
select * from {{ ref('thawks_integration__wks_thailand_regional_sellout_base') }}
),
edw_vw_os_time_dim as (
select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_vw_os_customer_dim as (
select * from {{ ref('thaedw_integration__edw_vw_th_customer_dim') }}
),
vw_edw_reg_exch_rate as 
(select * from {{ ref('aspedw_integration__vw_edw_reg_exch_rate') }}
),
th_product_selection as (
select * from {{ ref('thawks_integration__wks_th_product_selection') }}
),
transformed as (
select 
  cal."year" as year, 
  cast(cal.qrtr_no as varchar) as qrtr_no, 
  cast(cal.mnth_id as varchar) as mnth_id, 
  cal.mnth_no as mnth_no, 
  cal.cal_date, 
  sellout.day, 
  sellout.cntry_cd as country_code, 
  sellout.cntry_nm as country_name, 
  sellout.data_src as data_source, 
  trim(
    nvl (
      nullif(sellout.sold_to_code, ''), 
      'NA'
    )
  ) as soldto_code, 
  trim(
    nvl (
      nullif(sellout.distributor_code, ''), 
      'NA'
    )
  ) as distributor_code, 
  trim(
    nvl (
      nullif(sellout.distributor_name, ''), 
      'NA'
    )
  ) as distributor_name, 
  trim(
    nvl (
      nullif(sellout.store_code, ''), 
      'NA'
    )
  ) as store_code, 
  trim(
    nvl (
      nullif(sellout.store_name, ''), 
      'NA'
    )
  ) as store_name, 
  trim(
    nvl (
      nullif(sellout.store_type_code, ''), 
      'NA'
    )
  ) as store_type, 
  'NA' as distributor_additional_attribute1, 
  'NA' as distributor_additional_attribute2, 
  'NA' as distributor_additional_attribute3, 
  trim(
    nvl (
      nullif(sellout.sap_prnt_cust_key, ''), 
      'NA'
    )
  ) as sap_parent_customer_key, 
  upper(
    trim(
      nvl (
        nullif(t3.sap_prnt_cust_desc, ''), 
        'NA'
      )
    )
  ) as sap_parent_customer_description, 
  trim(
    nvl (
      nullif(t3.sap_cust_chnl_key, ''), 
      'NA'
    )
  ) as sap_customer_channel_key, 
  upper(
    trim(
      nvl (
        nullif(t3.sap_cust_chnl_desc, ''), 
        'NA'
      )
    )
  ) as sap_customer_channel_description, 
  trim(
    nvl (
      nullif(t3.sap_cust_sub_chnl_key, ''), 
      'NA'
    )
  ) as sap_customer_sub_channel_key, 
  upper(
    trim(
      nvl (
        nullif(t3.sap_sub_chnl_desc, ''), 
        'NA'
      )
    )
  ) as sap_sub_channel_description, 
  trim(
    nvl (
      nullif(t3.sap_go_to_mdl_key, ''), 
      'NA'
    )
  ) as sap_go_to_mdl_key, 
  upper(
    trim(
      nvl (
        nullif(t3.sap_go_to_mdl_desc, ''), 
        'NA'
      )
    )
  ) as sap_go_to_mdl_description, 
  trim(
    nvl (
      nullif(t3.sap_bnr_key, ''), 
      'NA'
    )
  ) as sap_banner_key, 
  upper(
    trim(
      nvl (
        nullif(t3.sap_bnr_desc, ''), 
        'NA'
      )
    )
  ) as sap_banner_description, 
  trim(
    nvl (
      nullif(t3.sap_bnr_frmt_key, ''), 
      'NA'
    )
  ) as sap_banner_format_key, 
  upper(
    trim(
      nvl (
        nullif(t3.sap_bnr_frmt_desc, ''), 
        'NA'
      )
    )
  ) as sap_banner_format_description, 
  trim(
    nvl (
      nullif(t3.retail_env, ''), 
      'NA'
    )
  ) as retail_environment, 
  --trim(nvl (nullif(t3.sap_region,''),'na')) as region,
  --trim(nvl (nullif(t3.sap_region,''),'na')) as zone_or_area,
  -- trim(nvl (nullif(t3.segmt_key,''),'na')) as customer_segment_key,
  -- trim(nvl (nullif(t3.segment_desc,''),'na')) as customer_segment_description,
  null as customer_segment_key, 
  null as customer_segment_description, 
  trim(
    nvl (
      nullif(t4.gph_prod_frnchse, ''), 
      'NA'
    )
  ) as global_product_franchise, 
  trim(
    nvl (
      nullif(t4.gph_prod_brnd, ''), 
      'NA'
    )
  ) as global_product_brand, 
  trim(
    nvl (
      nullif(t4.gph_prod_sub_brnd, ''), 
      'NA'
    )
  ) as global_product_sub_brand, 
  trim(
    nvl (
      nullif(t4.gph_prod_vrnt, ''), 
      'NA'
    )
  ) as global_product_variant, 
  trim(
    nvl (
      nullif(t4.gph_prod_sgmnt, ''), 
      'NA'
    )
  ) as global_product_segment, 
  trim(
    nvl (
      nullif(t4.gph_prod_subsgmnt, ''), 
      'NA'
    )
  ) as global_product_subsegment, 
  trim(
    nvl (
      nullif(t4.gph_prod_ctgry, ''), 
      'NA'
    )
  ) as global_product_category, 
  trim(
    nvl (
      nullif(t4.gph_prod_subctgry, ''), 
      'NA'
    )
  ) as global_product_subcategory, 
  trim(
    nvl (
      nullif(t4.gph_prod_put_up_desc, ''), 
      'NA'
    )
  ) as global_put_up_description, 
  sellout.ean as ean, 
  ltrim(t4.sku_cd, 0) as sku_code, 
  upper(t4.sap_mat_desc) as sku_description, 
  --trim(nvl (nullif(t4.greenlight_sku_flag,''),'na')) as greenlight_sku_flag,
  --trim(nvl (nullif(t4.pka_product_key,''),'na')) as pka_product_key,
  --trim(nvl (nullif(t4.pka_product_key_description,''),'na')) as pka_product_key_description,
  case when trim(
    nvl (
      nullif(t4.pka_product_key, ''), 
      'NA'
    )
  ) in ('N/A', 'NA') then 'NA' else trim(
    nvl (
      nullif(t4.pka_product_key, ''), 
      'NA'
    )
  ) end as pka_product_key, 
  case when trim(
    nvl (
      nullif(
        t4.pka_product_key_description, 
        ''
      ), 
      'NA'
    )
  ) in ('N/A', 'NA') then 'NA' else trim(
    nvl (
      nullif(
        t4.pka_product_key_description, 
        ''
      ), 
      'NA'
    )
  ) end as pka_product_key_description, 
  --trim(nvl (nullif(t4.sls_org,''),'na')) as sls_org,
  trim(
    nvl (
      nullif(
        sellout.customer_product_desc, ''
      ), 
      'NA'
    )
  ) as customer_product_desc, 
  trim(
    nvl (
      nullif(sellout.region, ''), 
      'NA'
    )
  ) as region, 
  trim(
    nvl (
      nullif(sellout.zone_or_area, ''), 
      'NA'
    )
  ) as zone_or_area, 
  th_cur.from_ccy as from_currency, 
  th_cur.to_ccy as to_currecy, 
  --th_cur.exch_rate as exchange_rate,
  (
    th_cur.exch_rate /(
      th_cur.from_ratio * th_cur.to_ratio
    )
  ):: numeric(15, 5) as exchange_rate, 
  sum(sellout.so_sls_qty) as sellout_sales_quantity, 
  sum(sellout.so_sls_value) as sellout_sales_value, 
  sum(
    so_sls_value *(
      th_cur.exch_rate /(
        th_cur.from_ratio * th_cur.to_ratio
      )
    ):: numeric(15, 5)
  ):: numeric(38, 11) as sellout_sales_value_usd, 
  trim(
    nvl (
      nullif(sellout.msl_product_code, ''), 
      'na'
    )
  ) as msl_product_code, 
  trim(
    nvl (
      nullif(t4.sap_mat_desc, ''), 
      'NA'
    )
  ) as msl_product_desc, 
  --trim(nvl (nullif(sellout.store_grade,''),'na')) as store_grade,
  trim(
    nvl (
      nullif(sellout.retail_env, ''), 
      'NA'
    )
  ) as retail_env, 
  trim(
    nvl (
      nullif(sellout.channel, ''), 
      'NA'
    )
  ) as channel, 
  sellout.crtd_dttm, 
  sellout.updt_dttm 
from 
  wks_thailand_regional_sellout_base sellout, 
  (
    select 
      distinct "year", 
      qrtr_no, 
      mnth_id, 
      mnth_no, 
      cal_date 
    from 
      edw_vw_os_time_dim
  ) cal, 
  --customer section
  (
    select 
      * 
    from 
      (
        select 
          distinct t3.sap_cust_id, 
          t3.sap_prnt_cust_key, 
          t3.sap_prnt_cust_desc, 
          t3.sap_cust_chnl_key, 
          t3.sap_cust_chnl_desc, 
          t3.sap_cust_sub_chnl_key, 
          t3.sap_sub_chnl_desc, 
          t3.sap_go_to_mdl_key, 
          t3.sap_go_to_mdl_desc, 
          t3.sap_bnr_key, 
          t3.sap_bnr_desc, 
          t3.sap_bnr_frmt_key, 
          t3.sap_bnr_frmt_desc, 
          --t3.segmt_key,
          --t3.segment_desc,
          t3.retail_env, 
          t3.sap_region, 
          row_number() over (
            partition by sap_prnt_cust_key 
            order by 
              sap_cust_id
          ) as rank 
        from 
          (
            select 
              * 
            from 
              edw_vw_os_customer_dim 
            where 
              sap_cntry_cd = 'TH' 
              and sap_prnt_cust_key != ''
          ) as t3
      ) 
    where 
      rank = 1
  ) t3, 
  ---product selection
  (
    select 
      * 
    from 
      th_product_selection
  ) t4, 
  (
    select 
      * 
    from 
      vw_edw_reg_exch_rate 
    where 
      cntry_key = 'TH' 
      and to_ccy = 'USD' 
      and jj_mnth_id = (
        select 
          max(jj_mnth_id) 
        from 
          vw_edw_reg_exch_rate
      )
  ) th_cur 
where 
  sellout.day = cal.cal_date 
  and sellout.mnth_id = cal.mnth_id 
  and sellout.sap_prnt_cust_key = t3.sap_prnt_cust_key(+) 
  and ltrim(sellout.sku_cd, 0)= ltrim(
    t4.sku_cd(+), 
    0
  ) 
  and th_cur.from_ccy = 'THB' 
group by 
  cal."year", 
  cal.qrtr_no, 
  cal.mnth_id, 
  cal.mnth_no, 
  cal.cal_date, 
  sellout.day, 
  sellout.cntry_cd, 
  sellout.cntry_nm, 
  sellout.data_src, 
  sellout.sold_to_code, 
  sellout.distributor_code, 
  sellout.distributor_name, 
  sellout.store_code, 
  sellout.store_name, 
  sellout.store_type_code, 
  sellout.sap_prnt_cust_key, 
  t3.sap_prnt_cust_desc, 
  t3.sap_cust_chnl_key, 
  t3.sap_cust_chnl_desc, 
  t3.sap_cust_sub_chnl_key, 
  t3.sap_sub_chnl_desc, 
  t3.sap_go_to_mdl_key, 
  t3.sap_go_to_mdl_desc, 
  t3.sap_bnr_key, 
  t3.sap_bnr_desc, 
  t3.sap_bnr_frmt_key, 
  t3.sap_bnr_frmt_desc, 
  t3.retail_env, 
  --t3.sap_region, 
  --t3.sap_region, 
  --t3.segmt_key, 
  --t3.segment_desc, 
  t4.gph_prod_frnchse, 
  t4.gph_prod_brnd, 
  t4.gph_prod_sub_brnd, 
  t4.gph_prod_vrnt, 
  t4.gph_prod_sgmnt, 
  t4.gph_prod_subsgmnt, 
  t4.gph_prod_ctgry, 
  t4.gph_prod_subctgry, 
  t4.gph_prod_put_up_desc, 
  sellout.ean, 
  t4.sku_cd, 
  t4.sap_mat_desc, 
  --t4.greenlight_sku_flag, 
  t4.pka_product_key, 
  t4.pka_product_key_description, 
  --t4.sls_org,
  sellout.customer_product_desc, 
  sellout.region, 
  sellout.zone_or_area, 
  th_cur.from_ccy, 
  th_cur.to_ccy, 
  --th_cur.exch_rate,
  (
    th_cur.exch_rate /(
      th_cur.from_ratio * th_cur.to_ratio
    )
  ), 
  sellout.msl_product_code, 
  --sellout.msl_product_desc,
  --sellout.store_grade,
  sellout.retail_env, 
  sellout.channel, 
  sellout.crtd_dttm, 
  sellout.updt_dttm
  ),
  final as (
  select 
    year::number(18,0) as year,
    qrtr_no::varchar(11) as qrtr_no,
    mnth_id::varchar(23) as mnth_id,
    mnth_no::number(18,0) as mnth_no,
    cal_date::date as cal_date,
    day::timestamp_ntz(9) as day,
    country_code::varchar(2) as country_code,
    country_name::varchar(8) as country_name,
    data_source::varchar(14) as data_source,
    soldto_code::varchar(255) as soldto_code,
    distributor_code::varchar(12) as distributor_code,
    distributor_name::varchar(100) as distributor_name,
    store_code::varchar(20) as store_code,
    store_name::varchar(500) as store_name,
    store_type::varchar(50) as store_type,
    distributor_additional_attribute1::varchar(2) as distributor_additional_attribute1,
    distributor_additional_attribute2::varchar(2) as distributor_additional_attribute2,
    distributor_additional_attribute3::varchar(2) as distributor_additional_attribute3,
    sap_parent_customer_key::varchar(12) as sap_parent_customer_key,
    sap_parent_customer_description::varchar(75) as sap_parent_customer_description,
    sap_customer_channel_key::varchar(12) as sap_customer_channel_key,
    sap_customer_channel_description::varchar(75) as sap_customer_channel_description,
    sap_customer_sub_channel_key::varchar(12) as sap_customer_sub_channel_key,
    sap_sub_channel_description::varchar(75) as sap_sub_channel_description,
    sap_go_to_mdl_key::varchar(12) as sap_go_to_mdl_key,
    sap_go_to_mdl_description::varchar(75) as sap_go_to_mdl_description,
    sap_banner_key::varchar(12) as sap_banner_key,
    sap_banner_description::varchar(75) as sap_banner_description,
    sap_banner_format_key::varchar(12) as sap_banner_format_key,
    sap_banner_format_description::varchar(75) as sap_banner_format_description,
    retail_environment::varchar(50) as retail_environment,
    customer_segment_key::varchar(1) as customer_segment_key,
    customer_segment_description::varchar(1) as customer_segment_description,
    global_product_franchise::varchar(30) as global_product_franchise,
    global_product_brand::varchar(30) as global_product_brand,
    global_product_sub_brand::varchar(100) as global_product_sub_brand,
    global_product_variant::varchar(100) as global_product_variant,
    global_product_segment::varchar(50) as global_product_segment,
    global_product_subsegment::varchar(100) as global_product_subsegment,
    global_product_category::varchar(50) as global_product_category,
    global_product_subcategory::varchar(50) as global_product_subcategory,
    global_put_up_description::varchar(100) as global_put_up_description,
    ean::varchar(20) as ean,
    sku_code::varchar(40) as sku_code,
    sku_description::varchar(150) as sku_description,
    pka_product_key::varchar(68) as pka_product_key,
    pka_product_key_description::varchar(255) as pka_product_key_description,
    customer_product_desc::varchar(500) as customer_product_desc,
    region::varchar(100) as region,
    zone_or_area::varchar(100) as zone_or_area,
    from_currency::varchar(5) as from_currency,
    to_currecy::varchar(5) as to_currecy,
    exchange_rate::number(15,5) as exchange_rate,
    sellout_sales_quantity::number(38,6) as sellout_sales_quantity,
    sellout_sales_value::number(38,6) as sellout_sales_value,
    sellout_sales_value_usd::number(38,11) as sellout_sales_value_usd,
    msl_product_code::varchar(20) as msl_product_code,
    msl_product_desc::varchar(100) as msl_product_desc,
    retail_env::varchar(300) as retail_env,
    channel::varchar(200) as channel,
    crtd_dttm::timestamp_ntz(9) as crtd_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
from transformed
)
select * from final