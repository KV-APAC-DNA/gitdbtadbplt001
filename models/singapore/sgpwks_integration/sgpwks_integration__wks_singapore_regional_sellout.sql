--sources--
with edw_vw_os_time_dim as (
    select * from {{ source('oseedw_access','edw_vw_os_time_dim') }}
),
wks_singapore_regional_sellout_base as (
  select * from {{ ref('sgpwks_integration__wks_singapore_regional_sellout_base') }}
),
edw_vw_greenlight_skus as (
  select * from {{ ref('aspedw_integration__edw_vw_greenlight_skus') }}
),
product_key_attributes as (
  select * from {{ ref('aspedw_integration__edw_product_key_attributes') }}
),
edw_gch_customerhierarchy as (
  select * from {{ ref('aspedw_integration__edw_gch_customerhierarchy') }}
),
edw_gch_producthierarchy as (
  select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
edw_customer_sales_dim as (
  select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
),
edw_material_sales_dim as (
  select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
edw_customer_base_dim as (
  select * from {{  ref('aspedw_integration__edw_customer_base_dim') }}
),
edw_company_dim  as (
  select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_dstrbtn_chnl as (
  select * from {{ ref('aspedw_integration__edw_dstrbtn_chnl')}}
),
edw_sales_org_dim as (
  select * from {{ ref('aspedw_integration__edw_sales_org_dim') }}
),
edw_code_descriptions as (
  select * from {{ ref('aspedw_integration__edw_code_descriptions') }}
),
edw_subchnl_retail_env_mapping as (
  select * from {{ source('aspedw_integration','edw_subchnl_retail_env_mapping') }}
),
edw_code_descriptions_manual as (
  select * from {{ source('aspedw_integration','edw_code_descriptions_manual') }}
),
vw_edw_reg_exch_rate as (
  select * from {{ ref('aspedw_integration__vw_edw_reg_exch_rate') }}
),
--Logical CTE--
c as 
(
    select
      *
    from vw_edw_reg_exch_rate
    where
      cntry_key = 'SG'
      and to_ccy = 'USD'
      and jj_mnth_id = (
        select
          max(jj_mnth_id)
        from vw_edw_reg_exch_rate
      )
  ),
cust as 
(
    select
      *
    from (
      select distinct
        ecbd.cust_num as sap_cust_id,
        ecbd.cust_nm as sap_cust_nm,
        ecsd.sls_org as sap_sls_org,
        ecd.company as sap_cmp_id,
        ecd.ctry_key as sap_cntry_cd,
        ecd.ctry_nm as sap_cntry_nm,
        ecsd.prnt_cust_key as sap_prnt_cust_key,
        cddes_pck.code_desc as sap_prnt_cust_desc,
        ecsd.chnl_key as sap_cust_chnl_key,
        cddes_chnl.code_desc as sap_cust_chnl_desc,
        ecsd.sub_chnl_key as sap_cust_sub_chnl_key,
        cddes_subchnl.code_desc as sap_sub_chnl_desc,
        ecsd.go_to_mdl_key as sap_go_to_mdl_key,
        cddes_gtm.code_desc as sap_go_to_mdl_desc,
        ecsd.bnr_key as sap_bnr_key,
        cddes_bnrkey.code_desc as sap_bnr_desc,
        ecsd.bnr_frmt_key as sap_bnr_frmt_key,
        cddes_bnrfmt.code_desc as sap_bnr_frmt_desc,
        subchnl_retail_env.retail_env,
        egch.gcgh_region as gch_region,
        egch.gcgh_cluster as gch_cluster,
        egch.gcgh_subcluster as gch_subcluster,
        egch.gcgh_market as gch_market,
        egch.gcch_retail_banner as gch_retail_banner,
        ecsd.segmt_key as cust_segmt_key,
        codes_segment.code_desc as cust_segment_desc,
        row_number() over (partition by sap_cust_id order by sap_prnt_cust_key, cust_segmt_key asc) as rank
      /* row_number() over (partition by sap_cust_id order by nullif(ecsd.prnt_cust_key,''),nullif(ecsd.bnr_key,''),nullif(ecsd.bnr_frmt_key,''),nullif(ecsd.segmt_key,'') asc nulls last) as rank */
      from edw_gch_customerhierarchy as egch, edw_customer_sales_dim as ecsd, edw_customer_base_dim as ecbd, edw_company_dim as ecd, edw_dstrbtn_chnl as edc, edw_sales_org_dim as esod, edw_code_descriptions as cddes_pck, edw_code_descriptions as cddes_bnrkey, edw_code_descriptions as cddes_bnrfmt, edw_code_descriptions as cddes_chnl, edw_code_descriptions as cddes_gtm, edw_code_descriptions as cddes_subchnl, edw_subchnl_retail_env_mapping as subchnl_retail_env, edw_code_descriptions_manual as codes_segment, (
        select distinct
          cust_num,
          rec_crt_dt,
          prnt_cust_key,
          row_number() over (partition by cust_num order by rec_crt_dt desc) as rn
        from edw_customer_sales_dim
      ) as a
      /* (select distinct customer_code,region_name,zone_name */ /* from in_edw.edw_customer_dim) regzone */
      where
        egch.customer = ecbd.cust_num
        and ecsd.cust_num = ecbd.cust_num
        and a.cust_num = ecsd.cust_num
        and ecsd.dstr_chnl = edc.distr_chan
        and ecsd.sls_org = esod.sls_org
        and esod.sls_org_co_cd = ecd.co_cd
        and a.rn = 1
        and upper(trim(cddes_pck.code_type)) = 'PARENT CUSTOMER KEY'
        and cddes_pck.code = ecsd.prnt_cust_key
        and upper(trim(cddes_bnrkey.code_type)) = 'BANNER KEY'
        and cddes_bnrkey.code = ecsd.bnr_key
        and upper(trim(cddes_bnrfmt.code_type)) = 'BANNER FORMAT KEY'
        and cddes_bnrfmt.code = ecsd.bnr_frmt_key
        and upper(trim(cddes_chnl.code_type)) = 'CHANNEL KEY'
        and cddes_chnl.code = ecsd.chnl_key
        and upper(trim(cddes_gtm.code_type)) = 'GO TO MODEL KEY'
        and cddes_gtm.code = ecsd.go_to_mdl_key
        and upper(trim(cddes_subchnl.code_type)) = 'SUB CHANNEL KEY'
        and cddes_subchnl.code = ecsd.sub_chnl_key
        and upper(subchnl_retail_env.sub_channel) = upper(cddes_subchnl.code_desc)
        and codes_segment.code_type = 'customer segmentation key'
        and codes_segment.code = ecsd.segmt_key
    )
    where
      rank = 1
  ),
  prod_key2 AS (
    select
      a.ctry_nm,
      a.ean_upc,
      a.sku,
      a.pka_productkey,
      a.pka_productdesc
    from (
      select
        ctry_nm,
        ltrim(ean_upc, '0') as ean_upc,
        ltrim(matl_num, '0') as sku,
        pka_productkey,
        pka_productdesc,
        lst_nts as nts_date
      from edw_product_key_attributes
      where
        (
          matl_type_cd = 'FERT' or matl_type_cd = 'HALB' or matl_type_cd = 'SAPR'
        )
        and not lst_nts is null
    ) as a
    join (
      select
        ctry_nm,
        ltrim(ean_upc, '0') as ean_upc,
        ltrim(matl_num, '0') as sku,
        lst_nts as latest_nts_date,
        row_number() over (partition by ctry_nm, ean_upc order by lst_nts desc) as row_number
      from edw_product_key_attributes
      where
        (
          matl_type_cd = 'FERT' or matl_type_cd = 'HALB' or matl_type_cd = 'SAPR'
        )
        and not lst_nts is null
    ) as b
      on a.ctry_nm = b.ctry_nm
      and a.ean_upc = b.ean_upc
      and a.sku = b.sku
      and b.latest_nts_date = a.nts_date
      and b.row_number = 1
      and a.ctry_nm = 'Singapore'
  ),
prod_key1 as 
(
    select
      a.ctry_nm,
      a.ean_upc,
      a.sku,
      a.pka_productkey,
      a.pka_productdesc
    from (
      select
        ctry_nm,
        ltrim(ean_upc, '0') as ean_upc,
        ltrim(matl_num, '0') as sku,
        pka_productkey,
        pka_productdesc,
        lst_nts as nts_date
      from edw_product_key_attributes
      where
        (
          matl_type_cd = 'FERT' or matl_type_cd = 'HALB' or matl_type_cd = 'SAPR'
        )
        and not lst_nts is null
    ) as a
    join (
      select
        ctry_nm,
        ltrim(ean_upc, '0') as ean_upc,
        ltrim(matl_num, '0') as sku,
        lst_nts as latest_nts_date,
        row_number() over (partition by ctry_nm, ean_upc order by lst_nts desc) as row_number
      from edw_product_key_attributes
      where
        (
          matl_type_cd = 'FERT' or matl_type_cd = 'HALB' or matl_type_cd = 'SAPR'
        )
        and not lst_nts is null
    ) as b
      on a.ctry_nm = b.ctry_nm
      and a.ean_upc = b.ean_upc
      and a.sku = b.sku
      and b.latest_nts_date = a.nts_date
      and b.row_number = 1
      and a.ctry_nm = 'Singapore'
  ) ,
  product as 
  (
    select distinct
      emd.matl_num as sap_matl_num,
      emd.matl_desc as sap_mat_desc,
      emd.matl_type_cd as sap_mat_type_cd,
      emd.matl_type_desc as sap_mat_type_desc,
      emd.prodh1 as sap_prod_sgmt_cd,
      emd.prodh1_txtmd as sap_prod_sgmt_desc,
      emd.base_prod_desc as sap_base_prod_desc,
      emd.mega_brnd_desc as sap_mega_brnd_desc,
      emd.brnd_desc as sap_brnd_desc,
      emd.varnt_desc as sap_vrnt_desc,
      emd.put_up_desc as sap_put_up_desc,
      emd.prodh2 as sap_grp_frnchse_cd,
      emd.prodh2_txtmd as sap_grp_frnchse_desc,
      emd.prodh3 as sap_frnchse_cd,
      emd.prodh3_txtmd as sap_frnchse_desc,
      emd.prodh4 as sap_prod_frnchse_cd,
      emd.prodh4_txtmd as sap_prod_frnchse_desc,
      emd.prodh5 as sap_prod_mjr_cd,
      emd.prodh5_txtmd as sap_prod_mjr_desc,
      emd.prodh5 as sap_prod_mnr_cd,
      emd.prodh5_txtmd as sap_prod_mnr_desc,
      emd.prodh6 as sap_prod_hier_cd,
      emd.prodh6_txtmd as sap_prod_hier_desc,
      emd.pka_product_key as pka_product_key,
      emd.pka_product_key_description as pka_product_key_description,
      egph."region" as gph_region,
      egph.regional_franchise as gph_reg_frnchse,
      egph.regional_franchise_group as gph_reg_frnchse_grp,
      egph.gcph_franchise as gph_prod_frnchse,
      egph.gcph_brand as gph_prod_brnd,
      egph.gcph_subbrand as gph_prod_sub_brnd,
      egph.gcph_variant as gph_prod_vrnt,
      egph.gcph_needstate as gph_prod_needstate,
      egph.gcph_category as gph_prod_ctgry,
      egph.gcph_subcategory as gph_prod_subctgry,
      egph.gcph_segment as gph_prod_sgmnt,
      egph.gcph_subsegment as gph_prod_subsgmnt,
      egph.put_up_code as gph_prod_put_up_cd,
      egph.put_up_description as gph_prod_put_up_desc,
      egph.size as gph_prod_size,
      egph.unit_of_measure as gph_prod_size_uom,
      row_number() over (partition by sap_matl_num order by sap_matl_num) as rnk
    from edw_material_dim as emd, edw_gch_producthierarchy as egph
    where
      ltrim(emd.matl_num, 0) = ltrim(egph.materialnumber, 0) and emd.prod_hier_cd <> ''
  ),
time as
(
    select distinct
      cal_year as year,
      cal_qrtr_no as qrtr_no,
      cal_mnth_id as mnth_id,
      cal_mnth_no as mnth_no
    from edw_vw_os_time_dim
  ),
transformed as (
  select
    cast(time.year as varchar(10)) as year,
    cast(time.qrtr_no as varchar(14)) as qrtr_no,
    cast(time.mnth_id as varchar(21)) as mnth_id,
    cast(time.mnth_no as varchar(10)) as mnth_no,
    sellout.day as cal_date,
    sellout.cntry_cd as country_code,
    sellout.cntry_nm as country_name,
    sellout.data_src as data_source,
    trim(coalesce(nullif(sellout.soldto_code, ''), 'NA')) as soldto_code,
    distributor_code,
    distributor_name,
    store_cd as store_code,
    trim(store_name) as store_name,
    trim(coalesce(nullif(sellout.store_type, ''), 'NA')) as store_type,
    'NA' as distributor_additional_attribute1,
    'NA' as distributor_additional_attribute2,
    'NA' as distributor_additional_attribute3,
    trim(coalesce(nullif(cust.sap_prnt_cust_key, ''), 'NA')) as sap_parent_customer_key,
    upper(trim(coalesce(nullif(cust.sap_prnt_cust_desc, ''), 'NA'))) as sap_parent_customer_description,
    trim(coalesce(nullif(cust.sap_cust_chnl_key, ''), 'NA')) as sap_customer_channel_key,
    upper(trim(coalesce(nullif(cust.sap_cust_chnl_desc, ''), 'NA'))) as sap_customer_channel_description,
    trim(coalesce(nullif(cust.sap_cust_sub_chnl_key, ''), 'NA')) as sap_customer_sub_channel_key,
    upper(trim(coalesce(nullif(cust.sap_sub_chnl_desc, ''), 'NA'))) as sap_sub_channel_description,
    trim(coalesce(nullif(cust.sap_go_to_mdl_key, ''), 'NA')) as sap_go_to_mdl_key,
    upper(trim(coalesce(nullif(cust.sap_go_to_mdl_desc, ''), 'NA'))) as sap_go_to_mdl_description,
    trim(coalesce(nullif(cust.sap_bnr_key, ''), 'NA')) as sap_banner_key,
    upper(trim(coalesce(nullif(cust.sap_bnr_desc, ''), 'NA'))) as sap_banner_description,
    trim(coalesce(nullif(cust.sap_bnr_frmt_key, ''), 'NA')) as sap_banner_format_key,
    upper(trim(coalesce(nullif(cust.sap_bnr_frmt_desc, ''), 'NA'))) as sap_banner_format_description,
    trim(coalesce(nullif(cust.retail_env, ''), 'NA')) as retail_environment,
    trim(coalesce(nullif(cust.cust_segmt_key, ''), 'NA')) /* trim(nvl (nullif(cust.region,''),'NA')) as region, */ /* trim(nvl (nullif(cust.zone_or_area,''),'NA')) as zone_or_area, */ as customer_segment_key,
    trim(coalesce(nullif(cust.cust_segment_desc, ''), 'NA')) as customer_segment_description,
    trim(coalesce(nullif(product.gph_prod_frnchse, ''), 'NA')) as global_product_franchise,
    trim(coalesce(nullif(product.gph_prod_brnd, ''), 'NA')) as global_product_brand,
    trim(coalesce(nullif(product.gph_prod_sub_brnd, ''), 'NA')) as global_product_sub_brand,
    trim(coalesce(nullif(product.gph_prod_vrnt, ''), 'NA')) as global_product_variant,
    trim(coalesce(nullif(product.gph_prod_sgmnt, ''), 'NA')) as global_product_segment,
    trim(coalesce(nullif(product.gph_prod_subsgmnt, ''), 'NA')) as global_product_subsegment,
    trim(coalesce(nullif(product.gph_prod_ctgry, ''), 'NA')) as global_product_category,
    trim(coalesce(nullif(product.gph_prod_subctgry, ''), 'NA')) as global_product_subcategory,
    trim(coalesce(nullif(product.gph_prod_put_up_desc, ''), 'NA')) as global_put_up_description,
    sellout.ean as ean,
    trim(coalesce(nullif(product.sap_matl_num, ''), 'NA')) as sku_code,
    upper(trim(coalesce(nullif(product.sap_mat_desc, ''), 'NA'))) as sku_description,
    case
      when not trim(coalesce(nullif(sellout.pka_product_key, ''), 'NA')) in ('N/A', 'NA')
      then trim(coalesce(nullif(sellout.pka_product_key, ''), 'NA'))
      when not trim(coalesce(nullif(product.pka_product_key, ''), 'NA')) in ('N/A', 'NA')
      then trim(coalesce(nullif(product.pka_product_key, ''), 'NA'))
      when not trim(coalesce(nullif(prod_key1.pka_productkey, ''), 'NA')) in ('N/A', 'NA')
      then trim(coalesce(nullif(prod_key1.pka_productkey, ''), 'NA'))
      when not trim(coalesce(nullif(prod_key2.pka_productkey, ''), 'NA')) in ('N/A', 'NA')
      then trim(coalesce(nullif(prod_key2.pka_productkey, ''), 'NA'))
      else trim(coalesce(nullif(product.pka_product_key, ''), 'NA'))
    end /* trim(nvl (nullif(product.greenlight_sku_flag,''),'NA')) as greenlight_sku_flag, */ as pka_product_key,
    case
      when not trim(coalesce(nullif(sellout.pka_product_key, ''), 'NA')) in ('N/A', 'NA')
      then trim(coalesce(nullif(sellout.pka_product_key_description, ''), 'NA'))
      when not trim(coalesce(nullif(product.pka_product_key, ''), 'NA')) in ('N/A', 'NA')
      then trim(coalesce(nullif(product.pka_product_key_description, ''), 'NA'))
      when not trim(coalesce(nullif(prod_key1.pka_productkey, ''), 'NA')) in ('N/A', 'NA')
      then trim(coalesce(nullif(prod_key1.pka_productdesc, ''), 'NA'))
      when not trim(coalesce(nullif(prod_key2.pka_productkey, ''), 'NA')) in ('N/A', 'NA')
      then trim(coalesce(nullif(prod_key2.pka_productdesc, ''), 'NA'))
      else trim(coalesce(nullif(product.pka_product_key_description, ''), 'NA'))
    end as pka_product_key_description,
    trim(coalesce(nullif(sellout.customer_product_desc, ''), 'NA')) /* trim(nvl (nullif(product.pka_product_key,''),'NA')) as pka_product_key, */ /* trim(nvl (nullif(product.pka_product_key_description,''),'NA')) as pka_product_key_description, */ /* trim(nvl (nullif(product.sls_org,''),'NA')) as sls_org, */ as customer_product_desc,
    trim(coalesce(nullif(sellout.region, ''), 'NA')) as region,
    trim(coalesce(nullif(sellout.zone_or_area, ''), 'NA')) as zone_or_area,
    cast('SGD' as varchar) as from_currency,
    'USD' as to_currency,
    cast((
      c.exch_rate / (
        c.from_ratio * c.to_ratio
      )
    ) /* c.exch_rate as exchange_rate, */ as decimal(15, 5)) as exchange_rate,
    sum(so_sls_qty) as sellout_sales_quantity,
    sum(so_sls_value) as sellout_sales_value,
    cast(sum(so_sls_value * (
      c.exch_rate / (
        c.from_ratio * c.to_ratio
      )
    )) as decimal(38, 11)) as sellout_sales_value_usd,
    sellout.master_code,
    sellout.crtd_dttm,
    sellout.updt_dttm
  from wks_singapore_regional_sellout_base as sellout
  left join time
    on sellout.mnth_id = time.mnth_id
  /* product selection */
  left join  product
    on ltrim(sellout.matl_num, '0') = ltrim(product.sap_matl_num, '0') and rnk = 1
  /* product key attribute selection */
  left join  prod_key1
    on ltrim(sellout.matl_num, '0') = ltrim(prod_key1.sku, '0')
  left join  prod_key2
    on ltrim(sellout.ean, '0') = ltrim(prod_key2.ean_upc, '0')
  /* -customer selection */
  left join cust
    on ltrim(sellout.soldto_code, '0') = ltrim(cust.sap_cust_id, '0')
  left join c
    on upper(sellout.cntry_nm) = upper(c.cntry_nm)
  where
    c.from_ccy = 'SGD'
  group by
    time.year,
    time.qrtr_no,
    sellout.day,
    sellout.cntry_cd,
    sellout.cntry_nm,
    time.mnth_id,
    time.mnth_no,
    concat(coalesce(cast(time.mnth_id as varchar(21)), ''), coalesce('01', '')),
    sellout.data_src,
    sellout.soldto_code,
    distributor_code,
    distributor_name,
    store_cd,
    store_name,
    store_type,
    distributor_additional_attribute1,
    distributor_additional_attribute2,
    distributor_additional_attribute3,
    cust.sap_prnt_cust_key,
    cust.sap_prnt_cust_desc,
    cust.sap_cust_chnl_key,
    cust.sap_cust_chnl_desc,
    cust.sap_cust_sub_chnl_key,
    cust.sap_sub_chnl_desc,
    cust.sap_go_to_mdl_key,
    cust.sap_go_to_mdl_desc,
    cust.sap_bnr_key,
    cust.sap_bnr_desc,
    cust.sap_bnr_frmt_key,
    cust.sap_bnr_frmt_desc,
    cust.retail_env,
    cust.cust_segmt_key,
    cust.cust_segment_desc,
    product.gph_prod_frnchse,
    product.gph_prod_brnd,
    product.gph_prod_sub_brnd,
    product.gph_prod_vrnt,
    product.gph_prod_sgmnt,
    product.gph_prod_subsgmnt,
    product.gph_prod_ctgry,
    product.gph_prod_subctgry,
    product.gph_prod_put_up_desc,
    sellout.ean,
    product.sap_matl_num,
    product.sap_mat_desc,
    case
      when not trim(coalesce(nullif(sellout.pka_product_key, ''), 'NA')) in ('N/A', 'NA')
      then trim(coalesce(nullif(sellout.pka_product_key, ''), 'NA'))
      when not trim(coalesce(nullif(product.pka_product_key, ''), 'NA')) in ('N/A', 'NA')
      then trim(coalesce(nullif(product.pka_product_key, ''), 'NA'))
      when not trim(coalesce(nullif(prod_key1.pka_productkey, ''), 'NA')) in ('N/A', 'NA')
      then trim(coalesce(nullif(prod_key1.pka_productkey, ''), 'NA'))
      when not trim(coalesce(nullif(prod_key2.pka_productkey, ''), 'NA')) in ('N/A', 'NA')
      then trim(coalesce(nullif(prod_key2.pka_productkey, ''), 'NA'))
      else trim(coalesce(nullif(product.pka_product_key, ''), 'NA'))
    end, /* product.greenlight_sku_flag, */ /* product.pka_product_key, */ /* product.pka_product_key_description, */
    case
      when not trim(coalesce(nullif(sellout.pka_product_key, ''), 'NA')) in ('N/A', 'NA')
      then trim(coalesce(nullif(sellout.pka_product_key_description, ''), 'NA'))
      when not trim(coalesce(nullif(product.pka_product_key, ''), 'NA')) in ('N/A', 'NA')
      then trim(coalesce(nullif(product.pka_product_key_description, ''), 'NA'))
      when not trim(coalesce(nullif(prod_key1.pka_productkey, ''), 'NA')) in ('N/A', 'NA')
      then trim(coalesce(nullif(prod_key1.pka_productdesc, ''), 'NA'))
      when not trim(coalesce(nullif(prod_key2.pka_productkey, ''), 'NA')) in ('N/A', 'NA')
      then trim(coalesce(nullif(prod_key2.pka_productdesc, ''), 'NA'))
      else trim(coalesce(nullif(product.pka_product_key_description, ''), 'NA'))
    end,
    sellout.customer_product_desc,
    sellout.region,
    sellout.zone_or_area,
    (
      c.exch_rate / (
        c.from_ratio * c.to_ratio
      )
    ), /* c.exch_rate  */
    sellout.master_code,
    sellout.crtd_dttm,
    sellout.updt_dttm
  having
    not (
      sum(sellout.so_sls_value) = 0 and sum(sellout.so_sls_qty) = 0
    )
),
final as (
select
  year::varchar(10) as year,
  qrtr_no::varchar(14) as qrtr_no,
  mnth_id::varchar(21) as mnth_id,
  mnth_no::varchar(10) as mnth_no,
  cal_date::date as cal_date,
  country_code::varchar(2) as country_code,
  country_name::varchar(9) as country_name,
  data_source::varchar(3) as data_source,
  soldto_code::varchar(200) as soldto_code,
  distributor_code::varchar(200) as distributor_code,
  distributor_name::varchar(10) as distributor_name,
  store_code::varchar(300) as store_code,
  store_name::varchar(300) as store_name,
  store_type::varchar(200) as store_type,
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
  region::varchar(2) as region,
  zone_or_area::varchar(2) as zone_or_area,
  customer_segment_key::varchar(12) as customer_segment_key,
  customer_segment_description::varchar(50) as customer_segment_description,
  global_product_franchise::varchar(30) as global_product_franchise,
  global_product_brand::varchar(30) as global_product_brand,
  global_product_sub_brand::varchar(100) as global_product_sub_brand,
  global_product_variant::varchar(100) as global_product_variant,
  global_product_segment::varchar(50) as global_product_segment,
  global_product_subsegment::varchar(100) as global_product_subsegment,
  global_product_category::varchar(50) as global_product_category,
  global_product_subcategory::varchar(50) as global_product_subcategory,
  global_put_up_description::varchar(100) as global_put_up_description,
  ean::varchar(255) as ean,
  sku_code::varchar(40) as sku_code,
  sku_description::varchar(150) as sku_description,
--   greenlight_sku_flag::varchar(300) as greenlight_sku_flag,
  case when pka_product_key in ('N/A', 'NA') then 'NA' else pka_product_key end as pka_product_key,
  case
  when pka_product_key_description::varchar(500) in ('N/A', 'NA')
  then 'NA'
  else pka_product_key_description::varchar(500)
  end as pka_product_key_description,
  from_currency::varchar(3) as from_currency, /* trim(nvl (nullif(product.sls_org,''),'NA')) as sls_org, */
  to_currency::varchar(3) as to_currency,
  exchange_rate::numeric(15,5) as exchange_rate,
  sellout_sales_quantity::numeric(38,0) as sellout_sales_quantity,
  sellout_sales_value::numeric(38,6) as sellout_sales_value,
  sellout_sales_value_usd::numeric(38,11) as sellout_sales_value_usd,
  master_code::varchar(255) as master_code,
  current_timestamp() as crtd_dttm,
  current_timestamp() as updt_dttm
  from transformed
)
select * from final