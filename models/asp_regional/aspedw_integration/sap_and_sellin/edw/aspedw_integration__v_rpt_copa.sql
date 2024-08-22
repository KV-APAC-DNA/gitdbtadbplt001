{{
    config(
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
    )
}}


with edw_copa_trans_fact as(
    select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
edw_calendar_dim as(
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
edw_company_dim as(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_material_dim as(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
v_intrm_reg_crncy_exch_fiscper as(
    select * from {{ ref('aspedw_integration__v_intrm_reg_crncy_exch_fiscper') }}
),
edw_invoice_fact as(
    select * from {{ ref('aspedw_integration__edw_invoice_fact') }}
),
v_edw_customer_sales_dim as(
    select * from {{ ref('aspedw_integration__v_edw_customer_sales_dim') }}
),
VW_DIM_GMC_SKU_MAPPINGS as(
    select * from {{ source('GLOBALMASTER_ACCESS','VW_DIM_GMC_SKU_MAPPINGS') }}
),
VW_DIM_GMC_ATTRIBUTE_MAPPINGS as(
    select * from {{ source('GLOBALMASTER_ACCESS','VW_DIM_GMC_ATTRIBUTE_MAPPINGS') }}
),
VW_DIM_GMC_GLOBAL_CATEGORY_HIER as(
    select * from {{ source('GLOBALMASTER_ACCESS','VW_DIM_GMC_GLOBAL_CATEGORY_HIER') }}
),
VW_DIM_GMC_GLOBAL_BRAND_HIER as(
    select * from {{ source('GLOBALMASTER_ACCESS','VW_DIM_GMC_GLOBAL_BRAND_HIER') }}
),
VW_DIM_GMC_PROFIT_CENTER_HIER as(
    select * from {{ source('GLOBALMASTER_ACCESS','VW_DIM_GMC_PROFIT_CENTER_HIER') }}
),
vw_itg_custgp_customer_hierarchy as(
    select * from {{ ref('aspitg_integration__vw_itg_custgp_customer_hierarchy') }}
),
transformed as(

  select
  main.prev_fisc_yr_per as prev_fisc_yr_per,
  main.latest_date as latest_date,
  main.latest_fisc_yrmnth as latest_fisc_yrmnth,
  main.fisc_yr as fisc_yr,
  main.fisc_yr_per as fisc_yr_per,
  main.fisc_day as fisc_day,
    case when trim(mat.mega_brnd_desc)='Dr Ci Labo' and main.ctry_nm='APSC Regional' then 'Travel Retail'
      when trim(mat.mega_brnd_desc)='Dr Ci Labo' and main.ctry_nm='Japan' then 'Japan DCL'
	  when trim(mat.mega_brnd_desc)='Dr Ci Labo' and main.ctry_nm='China Selfcare' then 'China Selfcare DCL'	
	  when trim(mat.mega_brnd_desc)='Dr Ci Labo' and main.ctry_nm='China Personal Care' then 'China Personal Care DCL'
	  when trim(mat.mega_brnd_desc)='Jupiter (PH to CH)' and main.ctry_nm='China Selfcare' then 'China Jupiter'
	  else main.ctry_nm end as ctry_nm,
  --main."cluster",
   case 
	   when trim(mat.mega_brnd_desc)='Dr Ci Labo' and 	main.ctry_nm in ('APSC Regional','Japan','China Selfcare','China Personal Care') then 'One DCL' 
	   when trim(main.ctry_nm) = 'APSC Regional' and trim(nvl(mat.mega_brnd_desc,'NA'))<>'Dr Ci Labo' then 'APSC Direct'
	   else main."cluster" end as "cluster", 
  main.obj_crncy_co_obj as obj_crncy_co_obj,
  IFF(mat.mega_brnd_desc='',null,mat.mega_brnd_desc) as "b1 mega-brand",

  gmc.B1_BRAND as BRAND,
  gmc.B2_SUBBRAND as SUBBRAND,
  gmc.C1_BUSINESS_SEGMENT as BUSINESS_SEGMENT,
  gmc.C2_BUSINESS_SUBSEGMENT as BUSINESS_SUBSEGMENT,
  gmc.C3_NEED_STATE as NEED_STATE,
  gmc.C4_CATEGORY as CATEGORY,
  gmc.C5_SUBCATEGORY as SUBCATEGORY,

  IFF(mat.brnd_desc='',null,mat.brnd_desc) as "b2 brand",
  IFF(mat.base_prod_desc='',null,mat.base_prod_desc) as "b3 base product",
  IFF(mat.varnt_desc='',null,mat.varnt_desc) as "b4 variant",
  IFF(mat.put_up_desc='',null,mat.put_up_desc) as "b5 put-up",
  IFF(mat.prodh1_txtmd='',null,mat.prodh1_txtmd) as "prod h1 operating group",
  IFF(mat.prodh2_txtmd='',null,mat.prodh2_txtmd) as "prod h2 franchise group",
  IFF(mat.prodh3_txtmd='',null,mat.prodh3_txtmd) as "prod h3 franchise",
  IFF(mat.prodh4_txtmd='',null,mat.prodh4_txtmd) as "prod h4 product franchise",
  IFF(mat.prodh5_txtmd='',null,mat.prodh5_txtmd) as "prod h5 product major",
  IFF(mat.prodh6_txtmd='',null,mat.prodh6_txtmd) as "prod h6 product minor",
  IFF(cus_sales_extn."parent customer"='',null,cus_sales_extn."parent customer") as "parent customer",
  IFF(cus_sales_extn.banner='',null,cus_sales_extn.banner) as banner,
  IFF(cus_sales_extn."banner format"='',null,cus_sales_extn."banner format") as "banner format",
  IFF(cus_sales_extn.channel='',null,cus_sales_extn.channel) as channel,
  IFF(cus_sales_extn."go to model"='',null,cus_sales_extn."go to model") as "go to model",
  IFF(cus_sales_extn."sub channel"='',null,cus_sales_extn."sub channel") as "sub channel",
  IFF(cus_sales_extn.retail_env='',null,cus_sales_extn.retail_env) as "retail_env",
  SUM(main.nts_usd) as nts_usd,
  SUM(main.nts_lcy) as nts_lcy,
  SUM(main.gts_usd) as gts_usd,
  SUM(main.gts_lcy) as gts_lcy,
  SUM(main.eq_usd) as eq_usd,
  SUM(main.eq_lcy) as eq_lcy,
  main.from_crncy as from_crncy,
  main.to_crncy as to_crncy,
  SUM(main.nts_qty) as nts_qty,
  SUM(main.gts_qty) as gts_qty,
  SUM(main.eq_qty) as eq_qty,
  SUM(main.ord_pc_qty) as ord_pc_qty,
  SUM(main.unspp_qty) as unspp_qty,
  main.cust_num as cust_num,
  nvl(cust.customer_segmentation,'Not Available') as customer_segmentation
FROM (
  (
    (
      (
        SELECT
          cast((((
                cast((
                  cast((
                    DATE_PART(
                      'YEAR',
                      (
                        TO_DATE(
                          (
                            cast((
                              cast((
                                calendar.fisc_yr
                              ) as VARCHAR)
                            ) as TEXT) || SUBSTRING(cast((
                              cast((
                                calendar.fisc_per
                              ) as VARCHAR)
                            ) as TEXT), 6)
                          ),
                          cast((
                            cast('yyyymm' as VARCHAR)
                          ) as TEXT)
                        ) -1
                      )
                    )
                  ) as VARCHAR)
                ) as TEXT) || cast((
                  cast('0' as VARCHAR)
                ) as TEXT)
              ) || cast((
                cast((
                  DATE_PART(
                   'MONTH',
                    (
                      TO_DATE(
                        (
                          cast((
                            cast((
                              calendar.fisc_yr
                            ) AS VARCHAR)
                          ) AS TEXT) || SUBSTRING(cast((
                            cast((
                              calendar.fisc_per
                            ) AS VARCHAR)
                          ) AS TEXT), 6)
                        ),
                        cast((
                          cast('yyyymm' AS VARCHAR)
                        ) AS TEXT)
                      ) - 1
                    )
                  )
                ) AS VARCHAR)
              ) AS TEXT)
            )
          ) AS VARCHAR) AS prev_fisc_yr_per,
            TO_CHAR(
           CONVERT_TIMEZONE(
             'Asia/Singapore',
             CURRENT_TIMESTAMP::TIMESTAMP_NTZ
           )::TIMESTAMP_NTZ,
           'YYYYMMDD'
       ) AS latest_date,
          cast((
            (
              cast((
                cast((
                  calendar.fisc_yr
                ) AS VARCHAR)
              ) AS TEXT) || SUBSTRING(cast((
                cast((
                  calendar.fisc_per
                ) AS VARCHAR)
              ) AS TEXT), 6)
            )
          ) AS VARCHAR) AS latest_fisc_yrmnth,
          copa.fisc_yr,
          copa.fisc_yr_per,
          TO_DATE(
            (
              (
                SUBSTRING(cast((
                  cast((
                    copa.fisc_yr_per
                  ) AS VARCHAR)
                ) AS TEXT), 6, 8) || cast((
                  cast('01' AS VARCHAR)
                ) AS TEXT)
              ) || SUBSTRING(cast((
                cast((
                  copa.fisc_yr_per
                ) AS VARCHAR)
              ) AS TEXT), 1, 4)
            ),
            cast((
              cast('MMDDYYYY' AS VARCHAR)
            ) AS TEXT)
          ) AS fisc_day,
          CASE
            WHEN (
              (
                (
                  (
                    (
                      (
                        (
                          LTRIM(
                            cast((
                              copa.cust_num
                            ) AS TEXT),
                            cast((
                              cast((
                                0
                              ) AS VARCHAR)
                            ) AS TEXT)
                          ) = cast((
                            cast('134559' AS VARCHAR)
                          ) AS TEXT)
                        )
                        OR (
                          LTRIM(
                            cast((
                              copa.cust_num
                            ) AS TEXT),
                            cast((
                              cast((
                                0
                              ) AS VARCHAR)
                            ) AS TEXT)
                          ) = cast((
                            cast('134106' AS VARCHAR)
                          ) AS TEXT)
                        )
                      )
                      OR (
                        LTRIM(
                          cast((
                            copa.cust_num
                          ) AS TEXT),
                          cast((
                            cast((
                              0
                            ) AS VARCHAR)
                          ) AS TEXT)
                        ) = cast((
                          cast('134258' AS VARCHAR)
                        ) AS TEXT)
                      )
                    )
                    OR (
                      LTRIM(
                        cast((
                          copa.cust_num
                        ) AS TEXT),
                        cast((
                          cast((
                            0
                          ) AS VARCHAR)
                        ) AS TEXT)
                      ) = cast((
                        cast('134855' AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
                  AND (
                    LTRIM(
                      cast((
                        copa.acct_num
                      ) AS TEXT),
                      cast((
                        cast((
                          0
                        ) AS VARCHAR)
                      ) AS TEXT)
                    ) <> cast((
                      cast('403185' AS VARCHAR)
                    ) AS TEXT)
                  )
                )
                AND (
                  cast((
                    mat.mega_brnd_desc
                  ) AS TEXT) <> cast((
                    cast('Vogue Int''l' AS VARCHAR)
                  ) AS TEXT)
                )
              )
              AND (
                copa.fisc_yr = 2018
              )
            )
            THEN cast('China Selfcare' AS VARCHAR)
            ELSE cmp.ctry_group
          END AS ctry_nm,
          CASE
            WHEN (
              (
                (
                  (
                    (
                      (
                        (
                          LTRIM(
                            cast((
                              copa.cust_num
                            ) AS TEXT),
                            cast((
                              cast((
                                0
                              ) AS VARCHAR)
                            ) AS TEXT)
                          ) = cast((
                            cast('134559' AS VARCHAR)
                          ) AS TEXT)
                        )
                        OR (
                          LTRIM(
                            cast((
                              copa.cust_num
                            ) AS TEXT),
                            cast((
                              cast((
                                0
                              ) AS VARCHAR)
                            ) AS TEXT)
                          ) = cast((
                            cast('134106' AS VARCHAR)
                          ) AS TEXT)
                        )
                      )
                      OR (
                        LTRIM(
                          cast((
                            copa.cust_num
                          ) AS TEXT),
                          cast((
                            cast((
                              0
                            ) AS VARCHAR)
                          ) AS TEXT)
                        ) = cast((
                          cast('134258' AS VARCHAR)
                        ) AS TEXT)
                      )
                    )
                    OR (
                      LTRIM(
                        cast((
                          copa.cust_num
                        ) AS TEXT),
                        cast((
                          cast((
                            0
                          ) AS VARCHAR)
                        ) AS TEXT)
                      ) = cast((
                        cast('134855' AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
                  AND (
                    LTRIM(
                      cast((
                        copa.acct_num
                      ) AS TEXT),
                      cast((
                        cast((
                          0
                        ) AS VARCHAR)
                      ) AS TEXT)
                    ) <> cast((
                      cast('403185' AS VARCHAR)
                    ) AS TEXT)
                  )
                )
                AND (
                  cast((
                    mat.mega_brnd_desc
                  ) AS TEXT) <> cast((
                    cast('Vogue Int''l' AS VARCHAR)
                  ) AS TEXT)
                )
              )
              AND (
                copa.fisc_yr = 2018
              )
            )
            THEN cast('China' AS VARCHAR)
            ELSE cmp."cluster"
          END AS "cluster",
          CASE
            WHEN (
              cast((
                cmp.ctry_group
              ) AS TEXT) = cast((
                cast('India' AS VARCHAR)
              ) AS TEXT)
            )
            THEN cast('INR' AS VARCHAR)
            WHEN (
              cast((
                cmp.ctry_group
              ) AS TEXT) = cast((
                cast('Philippines' AS VARCHAR)
              ) AS TEXT)
            )
            THEN cast('PHP' AS VARCHAR)
            WHEN (
              (
                cast((
                  cmp.ctry_group
                ) AS TEXT) = cast((
                  cast('China Selfcare' AS VARCHAR)
                ) AS TEXT)
              )
              OR (
                cast((
                  cmp.ctry_group
                ) AS TEXT) = cast((
                  cast('China Personal Care' AS VARCHAR)
                ) AS TEXT)
              )
            )
            THEN cast('RMB' AS VARCHAR)
            ELSE copa.obj_crncy_co_obj
          END AS obj_crncy_co_obj,
          copa.matl_num,
          copa.co_cd,
          CASE
            WHEN (
              (
                LTRIM(cast((
                  copa.cust_num
                ) AS TEXT), cast((
                  cast('0' AS VARCHAR)
                ) AS TEXT)) = cast((
                  cast('135520' AS VARCHAR)
                ) AS TEXT)
              )
              AND (
                (
                  cast((
                    copa.co_cd
                  ) AS TEXT) = cast((
                    cast('703A' AS VARCHAR)
                  ) AS TEXT)
                )
                OR (
                  cast((
                    copa.co_cd
                  ) AS TEXT) = cast((
                    cast('8888' AS VARCHAR)
                  ) AS TEXT)
                )
              )
            )
            THEN cast('100A' AS VARCHAR)
            ELSE copa.sls_org
          END AS sls_org,
          CASE
            WHEN (
              (
                LTRIM(cast((
                  copa.cust_num
                ) AS TEXT), cast((
                  cast('0' AS VARCHAR)
                ) AS TEXT)) = cast((
                  cast('135520' AS VARCHAR)
                ) AS TEXT)
              )
              AND (
                (
                  cast((
                    copa.co_cd
                  ) AS TEXT) = cast((
                    cast('703A' AS VARCHAR)
                  ) AS TEXT)
                )
                OR (
                  cast((
                    copa.co_cd
                  ) AS TEXT) = cast((
                    cast('8888' AS VARCHAR)
                  ) AS TEXT)
                )
              )
            )
            THEN cast('15' AS VARCHAR)
            ELSE copa.dstr_chnl
          END AS dstr_chnl,
          copa.div,
          copa.cust_num,
          CASE
            WHEN (
              (
                cast((
                  copa.acct_hier_shrt_desc
                ) AS TEXT) = cast((
                  cast('NTS' AS VARCHAR)
                ) AS TEXT)
              )
              AND (
                cast((
                  exch_rate.to_crncy
                ) AS TEXT) = cast((
                  cast('USD' AS VARCHAR)
                ) AS TEXT)
              )
            )
            THEN SUM((
              copa.amt_obj_crncy * exch_rate.ex_rt
            ))
            ELSE cast((
              cast((
                0
              ) AS DECIMAL)
            ) AS DECIMAL(18, 0))
          END AS nts_usd,
          CASE
            WHEN (
              (
                cast((
                  copa.acct_hier_shrt_desc
                ) AS TEXT) = cast((
                  cast('NTS' AS VARCHAR)
                ) AS TEXT)
              )
              AND (
                cast((
                  exch_rate.to_crncy
                ) AS TEXT) = cast((
                  CASE
                    WHEN (
                      (
                        cast((
                          cmp.ctry_group
                        ) AS TEXT) = cast((
                          cast('India' AS VARCHAR)
                        ) AS TEXT)
                      )
                      OR (
                        (
                          cmp.ctry_group IS NULL
                        ) AND (
                          'India' IS NULL
                        )
                      )
                    )
                    THEN cast('INR' AS VARCHAR)
                    WHEN (
                      (
                        cast((
                          cmp.ctry_group
                        ) AS TEXT) = cast((
                          cast('Philippines' AS VARCHAR)
                        ) AS TEXT)
                      )
                      OR (
                        (
                          cmp.ctry_group IS NULL
                        ) AND (
                          'Philippines' IS NULL
                        )
                      )
                    )
                    THEN cast('PHP' AS VARCHAR)
                    WHEN (
                      (
                        cast((
                          cmp.ctry_group
                        ) AS TEXT) = cast((
                          cast('China Selfcare' AS VARCHAR)
                        ) AS TEXT)
                      )
                      OR (
                        (
                          cmp.ctry_group IS NULL
                        ) AND (
                          'China Selfcare' IS NULL
                        )
                      )
                    )
                    THEN cast('RMB' AS VARCHAR)
                    WHEN (
                      (
                        cast((
                          cmp.ctry_group
                        ) AS TEXT) = cast((
                          cast('China Personal Care' AS VARCHAR)
                        ) AS TEXT)
                      )
                      OR (
                        (
                          cmp.ctry_group IS NULL
                        ) AND (
                          'China Personal Care' IS NULL
                        )
                      )
                    )
                    THEN cast('RMB' AS VARCHAR)
                    ELSE copa.obj_crncy_co_obj
                  END
                ) AS TEXT)
              )
            )
            THEN SUM((
              copa.amt_obj_crncy * exch_rate.ex_rt
            ))
            ELSE cast((
              cast((
                0
              ) AS DECIMAL)
            ) AS DECIMAL(18, 0))
          END AS nts_lcy,
          CASE
            WHEN (
              (
                cast((
                  copa.acct_hier_shrt_desc
                ) AS TEXT) = cast((
                  cast('GTS' AS VARCHAR)
                ) AS TEXT)
              )
              AND (
                cast((
                  exch_rate.to_crncy
                ) AS TEXT) = cast((
                  cast('USD' AS VARCHAR)
                ) AS TEXT)
              )
            )
            THEN SUM((
              copa.amt_obj_crncy * exch_rate.ex_rt
            ))
            ELSE cast((
              cast((
                0
              ) AS DECIMAL)
            ) AS DECIMAL(18, 0))
          END AS gts_usd,
          CASE
            WHEN (
              (
                cast((
                  copa.acct_hier_shrt_desc
                ) AS TEXT) = cast((
                  cast('GTS' AS VARCHAR)
                ) AS TEXT)
              )
              AND (
                cast((
                  exch_rate.to_crncy
                ) AS TEXT) = cast((
                  CASE
                    WHEN (
                      (
                        cast((
                          cmp.ctry_group
                        ) AS TEXT) = cast((
                          cast('India' AS VARCHAR)
                        ) AS TEXT)
                      )
                      OR (
                        (
                          cmp.ctry_group IS NULL
                        ) AND (
                          'India' IS NULL
                        )
                      )
                    )
                    THEN cast('INR' AS VARCHAR)
                    WHEN (
                      (
                        cast((
                          cmp.ctry_group
                        ) AS TEXT) = cast((
                          cast('Philippines' AS VARCHAR)
                        ) AS TEXT)
                      )
                      OR (
                        (
                          cmp.ctry_group IS NULL
                        ) AND (
                          'Philippines' IS NULL
                        )
                      )
                    )
                    THEN cast('PHP' AS VARCHAR)
                    WHEN (
                      (
                        cast((
                          cmp.ctry_group
                        ) AS TEXT) = cast((
                          cast('China Selfcare' AS VARCHAR)
                        ) AS TEXT)
                      )
                      OR (
                        (
                          cmp.ctry_group IS NULL
                        ) AND (
                          'China Selfcare' IS NULL
                        )
                      )
                    )
                    THEN cast('RMB' AS VARCHAR)
                    WHEN (
                      (
                        cast((
                          cmp.ctry_group
                        ) AS TEXT) = cast((
                          cast('China Personal Care' AS VARCHAR)
                        ) AS TEXT)
                      )
                      OR (
                        (
                          cmp.ctry_group IS NULL
                        ) AND (
                          'China Personal Care' IS NULL
                        )
                      )
                    )
                    THEN cast('RMB' AS VARCHAR)
                    ELSE copa.obj_crncy_co_obj
                  END
                ) AS TEXT)
              )
            )
            THEN SUM((
              copa.amt_obj_crncy * exch_rate.ex_rt
            ))
            ELSE cast((
              cast((
                0
              ) AS DECIMAL)
            ) AS DECIMAL(18, 0))
          END AS gts_lcy,
          CASE
            WHEN (
              (
                cast((
                  copa.acct_hier_shrt_desc
                ) AS TEXT) = cast((
                  cast('EQ' AS VARCHAR)
                ) AS TEXT)
              )
              AND (
                cast((
                  exch_rate.to_crncy
                ) AS TEXT) = cast((
                  cast('USD' AS VARCHAR)
                ) AS TEXT)
              )
            )
            THEN SUM((
              copa.amt_obj_crncy * exch_rate.ex_rt
            ))
            ELSE cast((
              cast((
                0
              ) AS DECIMAL)
            ) AS DECIMAL(18, 0))
          END AS eq_usd,
          CASE
            WHEN (
              (
                cast((
                  copa.acct_hier_shrt_desc
                ) AS TEXT) = cast((
                  cast('EQ' AS VARCHAR)
                ) AS TEXT)
              )
              AND (
                cast((
                  exch_rate.to_crncy
                ) AS TEXT) = cast((
                  CASE
                    WHEN (
                      (
                        cast((
                          cmp.ctry_group
                        ) AS TEXT) = cast((
                          cast('India' AS VARCHAR)
                        ) AS TEXT)
                      )
                      OR (
                        (
                          cmp.ctry_group IS NULL
                        ) AND (
                          'India' IS NULL
                        )
                      )
                    )
                    THEN cast('INR' AS VARCHAR)
                    WHEN (
                      (
                        cast((
                          cmp.ctry_group
                        ) AS TEXT) = cast((
                          cast('Philippines' AS VARCHAR)
                        ) AS TEXT)
                      )
                      OR (
                        (
                          cmp.ctry_group IS NULL
                        ) AND (
                          'Philippines' IS NULL
                        )
                      )
                    )
                    THEN cast('PHP' AS VARCHAR)
                    WHEN (
                      (
                        cast((
                          cmp.ctry_group
                        ) AS TEXT) = cast((
                          cast('China Selfcare' AS VARCHAR)
                        ) AS TEXT)
                      )
                      OR (
                        (
                          cmp.ctry_group IS NULL
                        ) AND (
                          'China Selfcare' IS NULL
                        )
                      )
                    )
                    THEN cast('RMB' AS VARCHAR)
                    WHEN (
                      (
                        cast((
                          cmp.ctry_group
                        ) AS TEXT) = cast((
                          cast('China Personal Care' AS VARCHAR)
                        ) AS TEXT)
                      )
                      OR (
                        (
                          cmp.ctry_group IS NULL
                        ) AND (
                          'China Personal Care' IS NULL
                        )
                      )
                    )
                    THEN cast('RMB' AS VARCHAR)
                    ELSE copa.obj_crncy_co_obj
                  END
                ) AS TEXT)
              )
            )
            THEN SUM((
              copa.amt_obj_crncy * exch_rate.ex_rt
            ))
            ELSE cast((
              cast(NULL AS DECIMAL)
            ) AS DECIMAL(18, 0))
          END AS eq_lcy,
          CASE
            WHEN (
              cast((
                cmp.ctry_group
              ) AS TEXT) = cast((
                cast('India' AS VARCHAR)
              ) AS TEXT)
            )
            THEN cast('INR' AS VARCHAR)
            WHEN (
              cast((
                cmp.ctry_group
              ) AS TEXT) = cast((
                cast('Philippines' AS VARCHAR)
              ) AS TEXT)
            )
            THEN cast('PHP' AS VARCHAR)
            WHEN (
              (
                cast((
                  cmp.ctry_group
                ) AS TEXT) = cast((
                  cast('China Selfcare' AS VARCHAR)
                ) AS TEXT)
              )
              OR (
                cast((
                  cmp.ctry_group
                ) AS TEXT) = cast((
                  cast('China Personal Care' AS VARCHAR)
                ) AS TEXT)
              )
            )
            THEN cast('RMB' AS VARCHAR)
            ELSE exch_rate.from_crncy
          END AS from_crncy,
          exch_rate.to_crncy,
          CASE
            WHEN (
              (
                cast((
                  copa.acct_hier_shrt_desc
                ) AS TEXT) = cast((
                  cast('NTS' AS VARCHAR)
                ) AS TEXT)
              )
              AND (
                cast((
                  exch_rate.to_crncy
                ) AS TEXT) = cast((
                  cast('USD' AS VARCHAR)
                ) AS TEXT)
              )
            )
            THEN SUM(copa.qty)
            ELSE cast((
              cast((
                0
              ) AS DECIMAL)
            ) AS DECIMAL(18, 0))
          END AS nts_qty,
          CASE
            WHEN (
              (
                cast((
                  copa.acct_hier_shrt_desc
                ) AS TEXT) = cast((
                  cast('GTS' AS VARCHAR)
                ) AS TEXT)
              )
              AND (
                cast((
                  exch_rate.to_crncy
                ) AS TEXT) = cast((
                  cast('USD' AS VARCHAR)
                ) AS TEXT)
              )
            )
            THEN SUM(copa.qty)
            ELSE cast((
              cast((
                0
              ) AS DECIMAL)
            ) AS DECIMAL(18, 0))
          END AS gts_qty,
          CASE
            WHEN (
              (
                cast((
                  copa.acct_hier_shrt_desc
                ) AS TEXT) = cast((
                  cast('EQ' AS VARCHAR)
                ) AS TEXT)
              )
              AND (
                cast((
                  exch_rate.to_crncy
                ) AS TEXT) = cast((
                  cast('USD' AS VARCHAR)
                ) AS TEXT)
              )
            )
            THEN SUM(copa.qty)
            ELSE cast((
              cast((
                0
              ) AS DECIMAL)
            ) AS DECIMAL(18, 0))
          END AS eq_qty,
          0 AS ord_pc_qty,
          0 AS unspp_qty
        FROM (
          (
            (
              (
                edw_copa_trans_fact AS copa
                  LEFT JOIN edw_calendar_dim AS calendar
                    ON (
                      (
                        calendar.cal_day = TO_DATE(
                          TO_CHAR(
							CONVERT_TIMEZONE(
							'Asia/Singapore',
					CURRENT_TIMESTAMP::TIMESTAMP_NTZ
							)::TIMESTAMP_NTZ,
								'YYYY-MM-DD'
								)::TEXT,
								'YYYY-MM-DD'
                        )
                      )
                    )
              )
              LEFT JOIN edw_company_dim AS cmp
                ON (
                  (
                    cast((
                      copa.co_cd
                    ) AS TEXT) = cast((
                      cmp.co_cd
                    ) AS TEXT)
                  )
                )
            )
            LEFT JOIN edw_material_dim AS mat
              ON (
                (
                  cast((
                    copa.matl_num
                  ) AS TEXT) = cast((
                    mat.matl_num
                  ) AS TEXT)
                )
              )
          )
          LEFT JOIN v_intrm_reg_crncy_exch_fiscper AS exch_rate
            ON (
              (
                (
                  (
                    cast((
                      copa.obj_crncy_co_obj
                    ) AS TEXT) = cast((
                      exch_rate.from_crncy
                    ) AS TEXT)
                  )
                  AND (
                    copa.fisc_yr_per = exch_rate.fisc_per
                  )
                )
                AND CASE
                  WHEN (
                    cast((
                      exch_rate.to_crncy
                    ) AS TEXT) <> cast((
                      cast('USD' AS VARCHAR)
                    ) AS TEXT)
                  )
                  THEN (
                    cast((
                      exch_rate.to_crncy
                    ) AS TEXT) = cast((
                      CASE
                        WHEN (
                          (
                            cast((
                              cmp.ctry_group
                            ) AS TEXT) = cast((
                              cast('India' AS VARCHAR)
                            ) AS TEXT)
                          )
                          OR (
                            (
                              cmp.ctry_group IS NULL
                            ) AND (
                              'India' IS NULL
                            )
                          )
                        )
                        THEN cast('INR' AS VARCHAR)
                        WHEN (
                          (
                            cast((
                              cmp.ctry_group
                            ) AS TEXT) = cast((
                              cast('Philippines' AS VARCHAR)
                            ) AS TEXT)
                          )
                          OR (
                            (
                              cmp.ctry_group IS NULL
                            ) AND (
                              'Philippines' IS NULL
                            )
                          )
                        )
                        THEN cast('PHP' AS VARCHAR)
                        WHEN (
                          (
                            cast((
                              cmp.ctry_group
                            ) AS TEXT) = cast((
                              cast('China Selfcare' AS VARCHAR)
                            ) AS TEXT)
                          )
                          OR (
                            (
                              cmp.ctry_group IS NULL
                            ) AND (
                              'China Selfcare' IS NULL
                            )
                          )
                        )
                        THEN cast('RMB' AS VARCHAR)
                        WHEN (
                          (
                            cast((
                              cmp.ctry_group
                            ) AS TEXT) = cast((
                              cast('China Personal Care' AS VARCHAR)
                            ) AS TEXT)
                          )
                          OR (
                            (
                              cmp.ctry_group IS NULL
                            ) AND (
                              'China Personal Care' IS NULL
                            )
                          )
                        )
                        THEN cast('RMB' AS VARCHAR)
                        ELSE copa.obj_crncy_co_obj
                      END
                    ) AS TEXT)
                  )
                  ELSE (
                    cast((
                      exch_rate.to_crncy
                    ) AS TEXT) = cast((
                      cast('USD' AS VARCHAR)
                    ) AS TEXT)
                  )
                END
              )
            )
        )
        WHERE
          (
            (
              (
                (
                  cast((
                    copa.acct_hier_shrt_desc
                  ) AS TEXT) = cast((
                    cast('GTS' AS VARCHAR)
                  ) AS TEXT)
                )
                OR (
                  cast((
                    copa.acct_hier_shrt_desc
                  ) AS TEXT) = cast((
                    cast('NTS' AS VARCHAR)
                  ) AS TEXT)
                )
              )
              OR (
                cast((
                  copa.acct_hier_shrt_desc
                ) AS TEXT) = cast((
                  cast('EQ' AS VARCHAR)
                ) AS TEXT)
              )
            )
            AND (
              cast((
                cast((
                  copa.fisc_yr_per
                ) AS VARCHAR)
              ) AS TEXT) >= (
                (
                  (
                    cast((
                      cast((
                        (
                          DATE_PART(YEAR, CURRENT_DATE()) - 2
                        )
                      ) AS VARCHAR)
                    ) AS TEXT) || cast((
                      cast((
                        0
                      ) AS VARCHAR)
                    ) AS TEXT)
                  ) || cast((
                    cast((
                      0
                    ) AS VARCHAR)
                  ) AS TEXT)
                ) || cast((
                  cast((
                    1
                  ) AS VARCHAR)
                ) AS TEXT)
              )
            )
          )
        GROUP BY
          calendar.fisc_yr,
          calendar.fisc_per,
          copa.fisc_yr,
          copa.fisc_yr_per,
          copa.obj_crncy_co_obj,
          copa.matl_num,
          copa.co_cd,
          copa.sls_org,
          copa.dstr_chnl,
          copa.div,
          copa.cust_num,
          copa.acct_num,
          copa.acct_hier_shrt_desc,
          exch_rate.from_crncy,
          exch_rate.to_crncy,
          cmp.ctry_group,
          cmp."cluster",
          mat.mega_brnd_desc
        UNION ALL
        SELECT
          cast((((
                cast((
                  cast((
                    DATE_PART(
                      'YEAR',
                      (
                        TO_DATE(
                          (
                            cast((
                              cast((
                                calendar1.fisc_yr
                              ) AS VARCHAR)
                            ) AS TEXT) || SUBSTRING(cast((
                              cast((
                                calendar1.fisc_per
                              ) AS VARCHAR)
                            ) AS TEXT), 6)
                          ),
                          cast((
                            cast('yyyymm' AS VARCHAR)
                          ) AS TEXT)
                        ) -1
                      )
                    )
                  ) AS VARCHAR)
                ) AS TEXT) || cast((
                  cast('0' AS VARCHAR)
                ) AS TEXT)
              ) || cast((
                cast((
                  DATE_PART(
                   'MONTH',
                    (
                      TO_DATE(
                        (
                          cast((
                            cast((
                              calendar1.fisc_yr
                            ) AS VARCHAR)
                          ) AS TEXT) || SUBSTRING(cast((
                            cast((
                              calendar1.fisc_per
                            ) AS VARCHAR)
                          ) AS TEXT), 6)
                        ),
                        cast((
                          cast('yyyymm' AS VARCHAR)
                        ) AS TEXT)
                      ) - 1
                    )
                  )
                ) AS VARCHAR)
              ) AS TEXT)
            )
          ) AS VARCHAR) AS prev_fisc_yr_per,
          TO_CHAR(
           CONVERT_TIMEZONE(
             'Asia/Singapore',
             CURRENT_TIMESTAMP::TIMESTAMP_NTZ
           )::TIMESTAMP_NTZ,
           'YYYYMMDD'
       ) AS latest_date,
          cast((
            (
              cast((
                cast((
                  calendar1.fisc_yr
                ) AS VARCHAR)
              ) AS TEXT) || SUBSTRING(cast((
                cast((
                  calendar1.fisc_per
                ) AS VARCHAR)
              ) AS TEXT), 6)
            )
          ) AS VARCHAR) AS latest_fisc_yrmnth,
          cast((
            SUBSTRING(cast((
              cast((
                cal.fisc_per
              ) AS VARCHAR)
            ) AS TEXT), 1, 4)
          ) AS INT) AS fisc_yr,
          cal.fisc_per AS fisc_yr_per,
          TO_DATE(
            (
              (
                SUBSTRING(cast((
                  cast((
                    cal.fisc_per
                  ) AS VARCHAR)
                ) AS TEXT), 6, 8) || cast((
                  cast('01' AS VARCHAR)
                ) AS TEXT)
              ) || SUBSTRING(cast((
                cast((
                  cal.fisc_per
                ) AS VARCHAR)
              ) AS TEXT), 1, 4)
            ),
            cast((
              cast('MMDDYYYY' AS VARCHAR)
            ) AS TEXT)
          ) AS fisc_day,
          cmp.ctry_group AS ctry_nm,
          cmp."cluster",
          invc.curr_key AS obj_crncy_co_obj,
          invc.matl_num,
          invc.co_cd,
          invc.sls_org,
          invc.dstr_chnl,
          invc.div,
          invc.cust_num,
          SUM(0) AS nts_usd,
          SUM(0) AS nts_lcy,
          SUM(0) AS gts_usd,
          SUM(0) AS gts_lcy,
          SUM(0) AS eq_usd,
          SUM(0) AS eq_lcy,
          cast('N/A' AS VARCHAR) AS from_crncy,
          cast('N/A' AS VARCHAR) AS to_crncy,
          SUM(0) AS nts_qty,
          SUM(0) AS gts_qty,
          SUM(0) AS eq_qty,
          SUM(invc.ord_pc_qty) AS ord_pc_qty,
          SUM(invc.unspp_qty) AS unspp_qty
        FROM (
          (
            (
              edw_invoice_fact AS invc
                LEFT JOIN edw_company_dim AS cmp
                  ON (
                    (
                      cast((
                        invc.co_cd
                      ) AS TEXT) = cast((
                        cmp.co_cd
                      ) AS TEXT)
                    )
                  )
            )
            LEFT JOIN edw_calendar_dim AS cal
              ON (
                (
                  invc.rqst_delv_dt = cal.cal_day
                )
              )
          )
          LEFT JOIN edw_calendar_dim AS calendar1
            ON (
              (
                calendar1.cal_day = TO_DATE(
                    TO_CHAR(
						CONVERT_TIMEZONE(
						'Asia/Singapore',
				CURRENT_TIMESTAMP::TIMESTAMP_NTZ
				)::TIMESTAMP_NTZ,
				'YYYY-MM-DD'
					)::TEXT,
					'YYYY-MM-DD'
                )
              )
            )
        )
        WHERE
          (
            cast((
              cast((
                cal.fisc_per
              ) AS VARCHAR)
            ) AS TEXT) >= (
              (
                (
                  cast((
                    cast((
                      (
                        DATE_PART(YEAR, CURRENT_DATE()) - 2
                      )
                    ) AS VARCHAR)
                  ) AS TEXT) || cast((
                    cast((
                      0
                    ) AS VARCHAR)
                  ) AS TEXT)
                ) || cast((
                  cast((
                    0
                  ) AS VARCHAR)
                ) AS TEXT)
              ) || cast((
                cast((
                  1
                ) AS VARCHAR)
              ) AS TEXT)
            )
          )
        GROUP BY
          calendar1.fisc_yr,
          calendar1.fisc_per,
          cal.fisc_per,
          cmp.ctry_group,
          cmp."cluster",
          invc.curr_key,
          invc.matl_num,
          invc.co_cd,
          invc.sls_org,
          invc.dstr_chnl,
          invc.div,
          invc.cust_num
      ) AS main
      LEFT JOIN edw_material_dim AS mat
        ON (
          (
            cast((
              main.matl_num
            ) AS TEXT) = cast((
              mat.matl_num
            ) AS TEXT)
          )
        )
    )
    JOIN edw_company_dim AS company
      ON (
        (
          cast((
            main.co_cd
          ) AS TEXT) = cast((
            company.co_cd
          ) AS TEXT)
        )
      )
  )
  LEFT JOIN v_edw_customer_sales_dim AS cus_sales_extn
    ON (
      (
        (
          (
            (
              cast((
                main.sls_org
              ) AS TEXT) = cast((
                cus_sales_extn.sls_org
              ) AS TEXT)
            )
            AND (
              cast((
                main.dstr_chnl
              ) AS TEXT) = cast((
                cus_sales_extn.dstr_chnl
              ) AS TEXT)
            )
          )
          AND (
            cast((
              main.div
            ) AS TEXT) = cast((
              cus_sales_extn.div
            ) AS TEXT)
          )
        )
        AND (
          cast((
            main.cust_num
          ) AS TEXT) = cast((
            cus_sales_extn.cust_num
          ) AS TEXT)
        )
      )
    )
)
left join (
Select d.B1_BRAND,d.B2_SUBBRAND,c.C1_BUSINESS_SEGMENT,c.C2_BUSINESS_SUBSEGMENT,c.C3_NEED_STATE,c.C4_CATEGORY,c.C5_SUBCATEGORY,a.GMC_SKU_CODE  
FROM VW_DIM_GMC_SKU_MAPPINGS a 
LEFT OUTER JOIN VW_DIM_GMC_ATTRIBUTE_MAPPINGS b 
ON a.GMC_CODE = b.GMC_CODE 
LEFT OUTER JOIN VW_DIM_GMC_GLOBAL_CATEGORY_HIER c 
ON b.C5_SUBCATEGORY_CODE = c.C5_SUBCATEGORY_CODE 
LEFT OUTER JOIN VW_DIM_GMC_GLOBAL_BRAND_HIER d 
ON b.B2_SUBBRAND_CODE = d.B2_SUBBRAND_CODE 
LEFT OUTER JOIN VW_DIM_GMC_PROFIT_CENTER_HIER e
ON b.P4_BRAND_CATEGORY_CODE = e.P4_CODE )gmc on right(gmc.GMC_SKU_CODE,18)= main.matl_num	
left join vw_itg_custgp_customer_hierarchy cust on trim(upper(main.ctry_nm))=trim(upper(cust.ctry_nm)) and ltrim(cust.cust_num,0)=ltrim(main.cust_num,0)
GROUP BY
  main.prev_fisc_yr_per,
  main.latest_date,
  main.latest_fisc_yrmnth,
  main.fisc_yr,
  main.fisc_yr_per,
  main.fisc_day,
  case when trim(mat.mega_brnd_desc)='Dr Ci Labo' and main.ctry_nm='APSC Regional' then 'Travel Retail'
      when trim(mat.mega_brnd_desc)='Dr Ci Labo' and main.ctry_nm='Japan' then 'Japan DCL'
	  when trim(mat.mega_brnd_desc)='Dr Ci Labo' and main.ctry_nm='China Selfcare' then 'China Selfcare DCL'	
	  when trim(mat.mega_brnd_desc)='Dr Ci Labo' and main.ctry_nm='China Personal Care' then 'China Personal Care DCL'
	  when trim(mat.mega_brnd_desc)='Jupiter (PH to CH)' and main.ctry_nm='China Selfcare' then 'China Jupiter'
	  else main.ctry_nm end,
  case 
	   when trim(mat.mega_brnd_desc)='Dr Ci Labo' and 	main.ctry_nm in ('APSC Regional','Japan','China Selfcare','China Personal Care') then 'One DCL' 
	   when trim(main.ctry_nm) = 'APSC Regional' and trim(nvl(mat.mega_brnd_desc,'NA'))<>'Dr Ci Labo' then 'APSC Direct'
	   else main."cluster" end,
  main.obj_crncy_co_obj,
  IFF(mat.mega_brnd_desc='',null,mat.mega_brnd_desc),

  gmc.B1_BRAND,gmc.B2_SUBBRAND,gmc.C1_BUSINESS_SEGMENT,gmc.C2_BUSINESS_SUBSEGMENT,gmc.C3_NEED_STATE,gmc.C4_CATEGORY,gmc.C5_SUBCATEGORY,

  IFF(mat.brnd_desc='',null,mat.brnd_desc),
  IFF(mat.base_prod_desc='',null,mat.base_prod_desc),
  IFF(mat.varnt_desc='',null,mat.varnt_desc),
  IFF(mat.put_up_desc='',null,mat.put_up_desc),
  IFF(mat.prodh1_txtmd='',null,mat.prodh1_txtmd),
  IFF(mat.prodh2_txtmd='',null,mat.prodh2_txtmd),
  IFF(mat.prodh3_txtmd='',null,mat.prodh3_txtmd),
  IFF(mat.prodh4_txtmd='',null,mat.prodh4_txtmd),
  IFF(mat.prodh5_txtmd='',null,mat.prodh5_txtmd),
  IFF(mat.prodh6_txtmd='',null,mat.prodh6_txtmd),
  IFF(cus_sales_extn."parent customer"='',null,cus_sales_extn."parent customer"),
  IFF(cus_sales_extn.banner='',null,cus_sales_extn.banner),
  IFF(cus_sales_extn."banner format"='',null,cus_sales_extn."banner format"),
  IFF(cus_sales_extn.channel='',null,cus_sales_extn.channel),
  IFF(cus_sales_extn."go to model"='',null,cus_sales_extn."go to model"),
  IFF(cus_sales_extn."sub channel"='',null,cus_sales_extn."sub channel"),
  IFF(cus_sales_extn.retail_env='',null,cus_sales_extn.retail_env),
  main.from_crncy,
  main.to_crncy,
  main.cust_num,
  nvl(cust.customer_segmentation,'Not Available') 
)

select * from transformed