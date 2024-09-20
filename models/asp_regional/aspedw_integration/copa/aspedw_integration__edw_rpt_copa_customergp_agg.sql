with wks_rpt_copa_customergp_agg2 as
(
    select * from {{ ref('aspwks_integration__wks_rpt_copa_customergp_agg2') }}
),
edw_material_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
v_edw_customer_sales_dim as
(
    select * from {{ ref('aspedw_integration__v_edw_customer_sales_dim') }}
),
edw_gch_producthierarchy as
(
    select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
vw_itg_custgp_customer_hierarchy as
(
    select * from {{ ref('aspitg_integration__vw_itg_custgp_customer_hierarchy') }}
),
vw_itg_custgp_prod_hierarchy as
(
    select * from {{ ref('aspitg_integration__vw_itg_custgp_prod_hierarchy') }}
),
itg_mds_kr_product_hierarchy as
(
    select * from {{ ref('ntaitg_integration__itg_mds_kr_product_hierarchy') }}
),
itg_mds_custgp_portfolio_mapping as
(
    select * from {{ ref('aspitg_integration__itg_mds_custgp_portfolio_mapping') }}
),
final as
(
    Select fact.fisc_yr,
        fact.fisc_yr_per,
        fact.fisc_day,
        fact.ctry_nm,
        fact.cluster,
        fact.prft_ctr,
        fact.obj_crncy_co_obj,
        COALESCE(NULLIF(fact.matl_num,''), 'Not Available'::CHARACTER VARYING) AS matl_num,      
        COALESCE(NULLIF(mat.matl_desc,''),'Not Available'::CHARACTER VARYING) AS matl_desc,
        COALESCE(NULLIF(mat.matl_grp_cd,''),'Not Available'::CHARACTER VARYING) AS matl_grp_cd,
        COALESCE(NULLIF(mat.mega_brnd_desc,''),'Not Available'::CHARACTER VARYING) AS mega_brnd_desc,
        COALESCE(NULLIF(mat.brnd_desc,''),'Not Available'::CHARACTER VARYING) AS brnd_desc,
        COALESCE(NULLIF(mat.pka_sub_brand_desc,''),'Not Available'::CHARACTER VARYING) AS pka_sub_brand_desc,
        COALESCE(NULLIF(mat.prodh5_txtmd,''),'Not Available'::CHARACTER VARYING) AS sap_prod_mnr_desc,
        COALESCE(NULLIF(mat.varnt_desc,''),'Not Available'::CHARACTER VARYING) AS varnt_desc,
        COALESCE(NULLIF(mat.prodh3_txtmd,''),'Not Available'::CHARACTER VARYING) AS sap_frnchse_desc,
        COALESCE(NULLIF(mat.pka_size_desc,''),'Not Available'::CHARACTER VARYING) AS pka_size_desc,
        COALESCE(NULLIF(mat.pka_package_desc,''),'Not Available'::CHARACTER VARYING) AS pka_package_desc,
        COALESCE(NULLIF(mat.pka_product_key_description,''),'Not Available'::CHARACTER VARYING) AS pka_product_key_description,
        COALESCE(NULLIF(gph.gcph_franchise,''),'Not Available'::CHARACTER VARYING) AS gcph_franchise,
        COALESCE(NULLIF(gph.gcph_variant,''),'Not Available'::CHARACTER VARYING) AS gcph_variant,
        COALESCE(NULLIF(ppm.regional_portfolio,''),'Not Available'::CHARACTER VARYING) AS regional_portfolio,
        prodhierloc.loc_prod1,
        CASE 
        WHEN fact.ctry_nm = 'Korea' then kr_prod.local_brand_classification 
        else prodhierloc.loc_prod2 end as loc_prod2,
        prodhierloc.loc_prod3,
        prodhierloc.loc_prod4,
        prodhierloc.loc_prod5,
        prodhierloc.loc_prod6,
        prodhierloc.loc_prod7,
        prodhierloc.loc_prod8,
        prodhierloc.loc_prod9,
        prodhierloc.loc_prod10,
        COALESCE(NULLIF(ppm.market_portfolio,''),'Not Available'::CHARACTER VARYING) AS market_portfolio,
        COALESCE(NULLIF(fact.cust_num,''),'Not Available'::CHARACTER VARYING) AS cust_num,
        COALESCE(NULLIF(cus_sales_extn."parent customer",''),'Not Available'::CHARACTER VARYING) AS "parent customer",
        /*conditional mapping for Vietnam DKSH since the customer codes in COPA are unassigned*/
        CASE WHEN --fisc_yr = 2022 and 
        fact.ctry_nm = 'Vietnam' and (fact.cust_num is null or fact.cust_num = 'Not Available' or fact.cust_num = '' or fact.cust_num in ('140327','140328')) then 'DKSH OTC'
        else 
        COALESCE(NULLIF(cus_sales_extn.banner,''),'Not Available'::CHARACTER VARYING) end as banner,
        COALESCE(NULLIF(cus_sales_extn."banner format",''),'Not Available'::CHARACTER VARYING) AS "banner format",
        COALESCE(NULLIF(cus_sales_extn.channel,''),'Not Available'::CHARACTER VARYING) AS channel,
        COALESCE(NULLIF(cus_sales_extn."go to model",''),'Not Available'::CHARACTER VARYING) AS "go to model",
        COALESCE(NULLIF(cus_sales_extn."sub channel",''),'Not Available'::CHARACTER VARYING) AS "sub channel",
                    CASE WHEN --fisc_yr = 2022 --and 
                fact.ctry_nm = 'Vietnam' and (fact.cust_num is null or fact.cust_num = 'Not Available' or fact.cust_num = '') 
                then  'General Distributors / Wholesalers' 
                else COALESCE(NULLIF(cus_sales_extn.retail_env,''),'Not Available'::CHARACTER VARYING) end AS retail_env, 
            CASE WHEN --fisc_yr = 2022 --and 
                fact.ctry_nm = 'Vietnam' and (fact.cust_num is null or fact.cust_num = 'Not Available' or fact.cust_num = '') 
                then 'General Distributors / Wholesalers' 
                else custhierloc.loc_channel1 end as loc_channel1,
        custhierloc.loc_channel2,
        custhierloc.loc_channel3,
        custhierloc.loc_cust1,
        CASE WHEN --fisc_yr = 2022 --and 
        fact.ctry_nm = 'Vietnam' and (fact.cust_num is null or fact.cust_num = 'Not Available' or fact.cust_num = '' or 
        fact.cust_num in ('140327','140328')) then 'DKSH OTC'
        else
        custhierloc.loc_cust2 end as loc_cust2,
        CASE WHEN fact.ctry_nm = 'Vietnam' and (fact.cust_num is null or fact.cust_num = 'Not Available' or fact.cust_num = '' or fact.cust_num in ('140327','140328')) then 'DKSH OTC'
        else
        custhierloc.loc_cust3 end as loc_cust3,
        --custhierloc.loc_cust3,
        /*conditional mapping for Vietnam DKSH since the customer codes in COPA are unassigned*/
        CASE WHEN --fisc_yr = 2022 and 
        fact.ctry_nm = 'Vietnam' and 
        (fact.cust_num is null or fact.cust_num = 'Not Available' or fact.cust_num = '') then 'Core' 
        else COALESCE(NULLIF(custhierloc.customer_segmentation,''),'Not Available') end as customer_segmentation ,
        /*conditional mapping for Vietnam DKSH since the customer codes in COPA are unassigned*/
        CASE WHEN --fisc_yr = 2022 and 
        fact.ctry_nm = 'Vietnam' and 	(fact.cust_num is null or fact.cust_num = 'Not Available'
        or fact.cust_num = '') then 'Core' 
        else COALESCE(NULLIF(custhierloc.local_cust_segmentation,''),'Not Available') end as local_cust_segmentation ,
        COALESCE(NULLIF(custhierloc.local_cust_segmentation_2,''),'Not Available') as local_cust_segmentation_2,
        fact.ntstt_lcy,
        fact.ntstt_usd,
        fact.ntstp_lcy,
        fact.ntstp_usd,
        fact.nts_lcy,
        fact.nts_usd,
        fact.gts_lcy,
        fact.gts_usd,
        fact.cfreegood_lcy,
        fact.cfreegood_usd,
        fact.stdcogs_lcy,
        fact.stdcogs_usd,
        fact.rtn_lcy,
        fact.rtn_usd,
        fact.glhd_lcy,
        fact.glhd_usd,
        fact.py_ntstt_lcy,
        fact.py_ntstt_usd,
        fact.py_ntstp_lcy,
        fact.py_ntstp_usd,
        fact.py_nts_lcy,
        fact.py_nts_usd,
        fact.py_gts_lcy,
        fact.py_gts_usd,
        fact.py_cfreegood_lcy,
        fact.py_cfreegood_usd,
        fact.py_stdcogs_lcy,
        fact.py_stdcogs_usd,
        fact.py_rtn_lcy,
        fact.py_rtn_usd,
        fact.py_glhd_lcy,
        fact.py_glhd_usd
    from wks_rpt_copa_customergp_agg2 fact
    LEFT JOIN edw_material_dim mat ON fact.matl_num::TEXT = LTRIM (mat.matl_num,'0')
    LEFT JOIN (select sls_org,cust_num,
                                "parent customer",
                                banner,
                                "banner format",
                                channel, 
                                "go to model",
                                "sub channel",
                                retail_env
                                
        from
        (select sls_org,ltrim(cust_num,'0') as cust_num,
                                "parent customer",
                                banner,
                                "banner format",
                                channel,
                                "go to model",
                                "sub channel",
                                retail_env,
                                row_number() over( partition by sls_org,cust_num
                                                order by prnt_cust_key desc) rn 
        from v_edw_customer_sales_dim )where rn = 1) cus_sales_extn 
            ON case when ctry_nm = 'Thailand' then '2400' else fact.sls_org::TEXT end = cus_sales_extn.sls_org::TEXT
        AND fact.cust_num::TEXT = cus_sales_extn.cust_num

        LEFT JOIN edw_gch_producthierarchy gph
            ON CASE WHEN fact.matl_num::TEXT = ''::CHARACTER VARYING::TEXT
            OR fact.matl_num IS NULL THEN '0'::CHARACTER VARYING ELSE fact.matl_num END::TEXT = LTRIM (gph.materialnumber::TEXT,'0'::CHARACTER VARYING::TEXT)
        LEFT JOIN vw_itg_custgp_customer_hierarchy custhierloc on fact.ctry_nm = custhierloc.ctry_nm and 
                                                                    fact.cust_num = custhierloc.cust_num
        LEFT JOIN vw_itg_custgp_prod_hierarchy prodhierloc on fact.ctry_nm = prodhierloc.ctry_nm and 
                                                                    fact.matl_num = prodhierloc.matl_num
        LEFT JOIN itg_mds_kr_product_hierarchy kr_prod ON LTRIM (fact.prft_ctr,'0') = LTRIM (kr_prod.prft_Ctr,'0')
    LEFT JOIN (select distinct market,prft_ctr,regional_portfolio,market_portfolio from itg_mds_custgp_portfolio_mapping) ppm
    ON  fact.prft_ctr = ppm.prft_ctr
            AND lower(fact.ctry_nm) = case when lower(ppm.market) = 'singapore local' then 'singapore' else lower(ppm.market) end 
    where not (coalesce(nullif(ntstt_lcy,0),0) = 0 and 
        coalesce(nullif(ntstt_usd,0),0) = 0 and  
        coalesce(nullif(ntstp_lcy,0),0)= 0 and 
        coalesce(nullif(ntstp_usd,0),0) = 0 and 
            coalesce(nullif(nts_lcy,0),0) = 0 and 
        coalesce(nullif(nts_usd,0),0)  = 0 and 
            coalesce(nullif(gts_lcy,0),0) = 0 and  
        coalesce(nullif(gts_usd,0),0)  = 0 and  
        coalesce(nullif(cfreegood_lcy,0),0) = 0 and  
        coalesce(nullif(cfreegood_usd,0),0) = 0 and  
        coalesce(nullif(stdcogs_lcy,0),0) = 0 and 
        coalesce(nullif(stdcogs_usd,0),0) = 0 and 
            coalesce(nullif(rtn_lcy,0),0) = 0 and 
        coalesce(nullif(rtn_usd,0),0) = 0 and 
        coalesce(nullif(glhd_lcy,0),0) = 0 and 
        coalesce(nullif(glhd_usd,0),0) = 0 and 
        coalesce(nullif(py_ntstt_lcy,0),0)  = 0 and 
        coalesce(nullif(py_ntstt_usd,0),0)  = 0 and  
        coalesce(nullif(py_ntstp_lcy,0),0) = 0 and  
        coalesce(nullif(py_ntstp_usd,0),0)  = 0 and  
        coalesce(nullif(py_nts_lcy,0),0) = 0 and  
        coalesce(nullif(py_nts_usd,0),0)  = 0 and  
            coalesce(nullif(py_gts_lcy,0),0)  = 0  and
        coalesce(nullif(py_gts_usd,0),0)  = 0  and
        coalesce(nullif(py_cfreegood_lcy,0),0)  = 0 and 
        coalesce(nullif(py_cfreegood_usd,0),0)  = 0 and  
        coalesce(nullif(py_stdcogs_lcy,0),0)  = 0 and  
        coalesce(nullif(py_stdcogs_usd,0),0)  = 0 and  
        coalesce(nullif(py_rtn_lcy,0),0)  = 0 and  
        coalesce(nullif(py_rtn_usd,0),0)  = 0 and  
        coalesce(nullif(py_glhd_lcy,0),0)  = 0 and   
        coalesce(nullif(py_glhd_usd,0),0)  = 0)
)
select fisc_yr::number(38,0) as fisc_yr,
    fisc_yr_per::number(38,0) as fisc_yr_per,
    fisc_day::date as fisc_day,
    ctry_nm::varchar(40) as ctry_nm,
    cluster::varchar(100) as cluster,
    prft_ctr::varchar(65535) as prft_ctr,
    obj_crncy_co_obj::varchar(5) as obj_crncy_co_obj,
    matl_num::varchar(18) as matl_num,
    matl_desc::varchar(100) as matl_desc,
    matl_grp_cd::varchar(20) as matl_grp_cd,
    mega_brnd_desc::varchar(100) as mega_brnd_desc,
    brnd_desc::varchar(100) as brnd_desc,
    pka_sub_brand_desc::varchar(30) as pka_sub_brand_desc,
    sap_prod_mnr_desc::varchar(100) as sap_prod_mnr_desc,
    varnt_desc::varchar(100) as varnt_desc,
    sap_frnchse_desc::varchar(100) as sap_frnchse_desc,
    pka_size_desc::varchar(30) as pka_size_desc,
    pka_package_desc::varchar(30) as pka_package_desc,
    pka_product_key_description::varchar(255) as pka_product_key_description,
    gcph_franchise::varchar(30) as gcph_franchise,
    gcph_variant::varchar(100) as gcph_variant,
    regional_portfolio::varchar(500) as regional_portfolio,
    loc_prod1::varchar(500) as loc_prod1,
    loc_prod2::varchar(500) as loc_prod2,
    loc_prod3::varchar(500) as loc_prod3,
    loc_prod4::varchar(500) as loc_prod4,
    loc_prod5::varchar(500) as loc_prod5,
    loc_prod6::varchar(500) as loc_prod6,
    loc_prod7::varchar(100) as loc_prod7,
    loc_prod8::varchar(100) as loc_prod8,
    loc_prod9::varchar(255) as loc_prod9,
    loc_prod10::varchar(500) as loc_prod10,
    market_portfolio::varchar(500) as market_portfolio,
    cust_num::varchar(13) as cust_num,
    "parent customer"::varchar(50) as "parent customer",
    banner::varchar(50) as banner,
    "banner format"::varchar(50) as "banner format",
    channel::varchar(50) as channel,
    "go to model"::varchar(50) as "go to model",
    "sub channel"::varchar(50) as "sub channel",
    retail_env::varchar(50) as retail_env,
    loc_channel1::varchar(500) as loc_channel1,
    loc_channel2::varchar(500) as loc_channel2,
    loc_channel3::varchar(500) as loc_channel3,
    loc_cust1::varchar(500) as loc_cust1,
    loc_cust2::varchar(750) as loc_cust2,
    loc_cust3::varchar(500) as loc_cust3,
    customer_segmentation::varchar(500) as customer_segmentation,
    local_cust_segmentation::varchar(500) as local_cust_segmentation,
    local_cust_segmentation_2::varchar(500) as local_cust_segmentation_2,
    ntstt_lcy::number(38,15) as ntstt_lcy,
    ntstt_usd::number(38,18) as ntstt_usd,
    ntstp_lcy::number(38,15) as ntstp_lcy,
    ntstp_usd::number(38,18) as ntstp_usd,
    nts_lcy::number(38,15) as nts_lcy,
    nts_usd::number(38,18) as nts_usd,
    gts_lcy::number(38,15) as gts_lcy,
    gts_usd::number(38,18) as gts_usd,
    cfreegood_lcy::number(38,15) as cfreegood_lcy,
    cfreegood_usd::number(38,18) as cfreegood_usd,
    stdcogs_lcy::number(38,15) as stdcogs_lcy,
    stdcogs_usd::number(38,18) as stdcogs_usd,
    rtn_lcy::number(38,15) as rtn_lcy,
    rtn_usd::number(38,18) as rtn_usd,
    glhd_lcy::number(38,15) as glhd_lcy,
    glhd_usd::number(38,18) as glhd_usd,
    py_ntstt_lcy::number(38,15) as py_ntstt_lcy,
    py_ntstt_usd::number(38,18) as py_ntstt_usd,
    py_ntstp_lcy::number(38,15) as py_ntstp_lcy,
    py_ntstp_usd::number(38,18) as py_ntstp_usd,
    py_nts_lcy::number(38,15) as py_nts_lcy,
    py_nts_usd::number(38,18) as py_nts_usd,
    py_gts_lcy::number(38,15) as py_gts_lcy,
    py_gts_usd::number(38,18) as py_gts_usd,
    py_cfreegood_lcy::number(38,15) as py_cfreegood_lcy,
    py_cfreegood_usd::number(38,18) as py_cfreegood_usd,
    py_stdcogs_lcy::number(38,15) as py_stdcogs_lcy,
    py_stdcogs_usd::number(38,18) as py_stdcogs_usd,
    py_rtn_lcy::number(38,15) as py_rtn_lcy,
    py_rtn_usd::number(38,18) as py_rtn_usd,
    py_glhd_lcy::number(38,15) as py_glhd_lcy,
    py_glhd_usd::number(38,18) as py_glhd_usd
 from final