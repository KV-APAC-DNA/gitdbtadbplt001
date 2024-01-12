
with edw_company_dim as (
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_copa_trans_fact as (
    select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
edw_material_dim as (
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
v_intrm_reg_crncy_exch_fiscper as (
    select * from {{ ref('aspedw_integration__v_intrm_reg_crncy_exch_fiscper') }}
),
v_edw_customer_sales_dim as (
    select * from {{ ref('aspedw_integration__v_edw_customer_sales_dim') }}
),
edw_vw_greenlight_skus as (
    select * from {{ ref('aspedw_integration__edw_vw_greenlight_skus') }}
),
edw_code_descriptions_manual as (
    select * from {{ source('aspedw_integration', 'edw_code_descriptions_manual') }}
),
edw_customer_base_dim as (
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
edw_sales_org_dim as (
    select * from {{ ref('aspedw_integration__edw_sales_org_dim') }}
),
edw_dstrbtn_chnl as (
    select * from {{ ref('aspedw_integration__edw_dstrbtn_chnl') }}
),
edw_invoice_fact as (
    select * from {{ ref('aspedw_integration__edw_invoice_fact') }}
),
itg_otif_glbl_con_reporting as (
    select * from {{ source('aspitg_integration', 'itg_otif_glbl_con_reporting') }}
),
itg_mds_ap_sales_ops_map as (
    select * from {{ ref('aspitg_integration__itg_mds_ap_sales_ops_map') }}
),

final as(
    select
  copa."datasource",
  copa.fisc_yr,
  copa.fisc_yr_per,
  copa.fisc_day,
  copa.ctry_nm,
  copa.co_cd,
  copa.company_nm,
  copa.sls_org,
  sls_org_dim.sls_org_nm,
  copa.dstr_chnl,
  dist_chnl_dim.txtsh as dstr_chnl_nm,
  copa."CLUSTER" as "cluster",
  copa.obj_crncy_co_obj,
  cust_sales."parent customer",
  cust_sales.banner,
  cust_sales."banner format",
  cust_sales.channel,
  cust_sales."go to model",
  cust_sales."sub channel",
  cust_sales.retail_env,
  copa.cust_num,
  cust_dim.cust_nm AS customer_name,
  Case
    when (
      cast((
        copa.co_cd
      ) as text) = cast((
        cast('703A' as varchar)
      ) as text)
    )
    then cast('SE001' as varchar)
    else cust_sales.segmt_key
  end as segmt_key,
  case
    when (
      cast((
        copa.co_cd
      ) as text) = cast((
        cast('703A' as varchar)
      ) as text)
    )
    then cast('Lead' as varchar)
    else code_desc.code_desc
  end as segment,
  coalesce(gn.greenlight_sku_flag, cast('N/A' as varchar)) as greenlight_sku_flag,
  sum(copa.nts_usd) as nts_usd,
  sum(copa.nts_lcy) as nts_lcy,
  sum(copa.gts_usd) as gts_usd,
  sum(copa.gts_lcy) as gts_lcy,
  sum(copa.numerator) as numerator,
  sum(copa.denominator) as denominator
from (
  (
    (
      (
        (
          (
            (
              select
                cast('Sellin' as varchar) as "datasource",
                copa.fisc_yr,
                copa.fisc_yr_per,
                to_date(
                  (
                    (
                      substring(cast((
                        cast((
                          copa.fisc_yr_per
                        ) as varchar)
                      ) as text), 6, 8) || cast((
                        cast('01' as varchar)
                      ) as text)
                    ) || substring(cast((
                      cast((
                        copa.fisc_yr_per
                      ) as varchar)
                    ) as text), 1, 4)
                  ),
                  cast((
                    cast('MMDDYYYY' as varchar)
                  ) as text)
                ) as fisc_day,
                case
                  when (
                    (
                      (
                        (
                          (
                            (
                              (
                                ltrim(
                                  cast((
                                    copa.cust_num
                                  ) as text),
                                  cast((
                                    cast((
                                      0
                                    ) as varchar)
                                  ) as text)
                                ) = cast((
                                  cast('134559' as varchar)
                                ) as text)
                              )
                              or (
                                ltrim(
                                  cast((
                                    copa.cust_num
                                  ) as text),
                                  cast((
                                    cast((
                                      0
                                    ) as varchar)
                                  ) as text)
                                ) = cast((
                                  cast('134106' as varchar)
                                ) as text)
                              )
                            )
                            or (
                              ltrim(
                                cast((
                                  copa.cust_num
                                ) as text),
                                cast((
                                  cast((
                                    0
                                  ) as varchar)
                                ) as text)
                              ) = cast((
                                cast('134258' as varchar)
                              ) as text)
                            )
                          )
                          or (
                            ltrim(
                              cast((
                                copa.cust_num
                              ) as text),
                              cast((
                                cast((
                                  0
                                ) as varchar)
                              ) as text)
                            ) = cast((
                              cast('134855' as varchar)
                            ) as text)
                          )
                        )
                        and (
                          ltrim(
                            cast((
                              copa.acct_num
                            ) as text),
                            cast((
                              cast((
                                0
                              ) as varchar)
                            ) as text)
                          ) <> cast((
                            cast('403185' as varchar)
                          ) as text)
                        )
                      )
                      and (
                        cast((
                          mat.mega_brnd_desc
                        ) as text) <> cast((
                          cast('Vogue Int\' l ' as varchar)
                        ) as text)
                      )
                    )
                    and (
                      copa.fisc_yr = 2018
                    )
                  )
                  then cast(' China Selfcare ' as varchar)
                  else cmp.ctry_group
                end as ctry_nm,
                copa.matl_num,
                copa.co_cd,
                cmp.company_nm,
                copa.sls_org,
                copa.dstr_chnl,
                case
                  when (
                    (
                      (
                        (
                          (
                            (
                              (
                                ltrim(
                                  cast((
                                    copa.cust_num
                                  ) as text),
                                  cast((
                                    cast((
                                      0
                                    ) as varchar)
                                  ) as text)
                                ) = cast((
                                  cast(' 134559 ' as varchar)
                                ) as text)
                              )
                              or (
                                ltrim(
                                  cast((
                                    copa.cust_num
                                  ) as text),
                                  cast((
                                    cast((
                                      0
                                    ) as varchar)
                                  ) as text)
                                ) = cast((
                                  cast(' 134106 ' as varchar)
                                ) AS TEXT)
                              )
                            )
                            or (
                              ltrim(
                                cast((
                                  copa.cust_num
                                ) as text),
                                cast((
                                  cast((
                                    0
                                  ) as varchar)
                                ) as text)
                              ) = cast((
                                cast(' 134258 ' as varchar)
                              ) as text)
                            )
                          )
                          or (
                            ltrim(
                              cast((
                                copa.cust_num
                              ) as text),
                              cast((
                                cast((
                                  0
                                ) as varchar)
                              ) as text)
                            ) = cast((
                              cast(' 134855 ' as varchar)
                            ) as text)
                          )
                        )
                        and (
                          ltrim(
                            cast((
                              copa.acct_num
                            ) as text),
                            cast((
                              cast((
                                0
                              ) as varchar)
                            ) as text)
                          ) <> cast((
                            cast(' 403185 ' as varchar)
                          ) as text)
                        )
                      )
                      and (
                        cast((
                          mat.mega_brnd_desc
                        ) as text) <> cast((
                          cast(' Vogue Int\'l' as varchar)
                        ) as text)
                      )
                    )
                    and (
                      copa.fisc_yr = 2018
                    )
                  )
                  then cast('China' as varchar)
                  else cmp."CLUSTER"
                end as "CLUSTER",
                case
                  when (
                    cast((
                      cmp.ctry_group
                    ) as text) = cast((
                      cast('India' as varchar)
                    ) as text)
                  )
                  then cast('INR' as varchar)
                  when (
                    cast((
                      cmp.ctry_group
                    ) as text) = cast((
                      cast('Philippines' as varchar)
                    ) as text)
                  )
                  then cast('PHP' as varchar)
                  when (
                    (
                      cast((
                        cmp.ctry_group
                      ) as text) = cast((
                        cast('China Selfcare' as varchar)
                      ) as text)
                    )
                    or (
                      cast((
                        cmp.ctry_group
                      ) as text) = cast((
                        cast('China Personal Care' as varchar)
                      ) as text)
                    )
                  )
                  then cast('RMB' as varchar)
                  else copa.obj_crncy_co_obj
                end as obj_crncy_co_obj,
                copa.div,
                copa.cust_num,
                case
                  when (
                    (
                      cast((
                        copa.acct_hier_shrt_desc
                      ) as text) = cast((
                        cast('NTS' as varchar)
                      ) as text)
                    )
                    and (
                      cast((
                        exch_rate.to_crncy
                      ) as text) = cast((
                        cast('USD' as varchar)
                      ) as text)
                    )
                  )
                  then sum((
                    copa.amt_obj_crncy * exch_rate.ex_rt
                  ))
                  else cast((
                    cast((
                      0
                    ) as decimal)
                  ) as decimal(18, 0))
                end as nts_usd,
                case
                  when (
                    (
                      cast((
                        copa.acct_hier_shrt_desc
                      ) as text) = cast((
                        cast('NTS' as varchar)
                      ) as text)
                    )
                    and (
                      cast((
                        exch_rate.to_crncy
                      ) as text) = cast((
                        case
                          when (
                            (
                              cast((
                                cmp.ctry_group
                              ) as text) = cast((
                                cast('India' as varchar)
                              ) as text)
                            )
                            or (
                              (
                                cmp.ctry_group is null
                              ) and (
                                'India' is null
                              )
                            )
                          )
                          then cast('INR' as varchar)
                          when (
                            (
                              cast((
                                cmp.ctry_group
                              ) as text) = cast((
                                cast('Philippines' as varchar)
                              ) as text)
                            )
                            or (
                              (
                                cmp.ctry_group is null
                              ) and (
                                'Philippines' is null
                              )
                            )
                          )
                          then cast('PHP' as varchar)
                          when (
                            (
                              cast((
                                cmp.ctry_group
                              ) as text) = cast((
                                cast('China Selfcare' as varchar)
                              ) as text)
                            )
                            or (
                              (
                                cmp.ctry_group is null
                              ) and (
                                'China Selfcare' is null
                              )
                            )
                          )
                          then cast('RMB' as varchar)
                          when (
                            (
                              cast((
                                cmp.ctry_group
                              ) as text) = cast((
                                cast('China Personal Care' as varchar)
                              ) as text)
                            )
                            or (
                              (
                                cmp.ctry_group is null
                              ) AND (
                                'China Personal Care' is null
                              )
                            )
                          )
                          then cast('RMB' as varchar)
                          else copa.obj_crncy_co_obj
                        end
                      ) as text)
                    )
                  )
                  then sum((
                    copa.amt_obj_crncy * exch_rate.ex_rt
                  ))
                  else cast((
                    cast((
                      0
                    ) as decimal)
                  ) as decimal(18, 0))
                end as nts_lcy,
                case
                  when (
                    (
                      cast((
                        copa.acct_hier_shrt_desc
                      ) as text) = cast((
                        cast('GTS' as varchar)
                      ) as text)
                    )
                    and (
                      cast((
                        exch_rate.to_crncy
                      ) as text) = cast((
                        cast('USD' as varchar)
                      ) as text)
                    )
                  )
                  then sum((
                    copa.amt_obj_crncy * exch_rate.ex_rt
                  ))
                  else cast((
                    cast((
                      0
                    ) as decimal)
                  ) as decimal(18, 0))
                end as gts_usd,
                case
                  when (
                    (
                      cast((
                        copa.acct_hier_shrt_desc
                      ) as text) = cast((
                        cast('GTS' as varchar)
                      ) as text)
                    )
                    and (
                      cast((
                        exch_rate.to_crncy
                      ) as text) = cast((
                        case
                          when (
                            (
                              cast((
                                cmp.ctry_group
                              ) as text) = cast((
                                cast('India' as varchar)
                              ) as text)
                            )
                            or (
                              (
                                cmp.ctry_group is null
                              ) and (
                                'India' is null
                              )
                            )
                          )
                          then cast('INR' as varchar)
                          when (
                            (
                              cast((
                                cmp.ctry_group
                              ) as text) = cast((
                                cast('Philippines' as varchar)
                              ) as text)
                            )
                            or (
                              (
                                cmp.ctry_group is null
                              ) and (
                                'Philippines' is null
                              )
                            )
                          )
                          then cast('PHP' as varchar)
                          when (
                            (
                              cast((
                                cmp.ctry_group
                              ) as text) = cast((
                                cast('China Selfcare' as varchar)
                              ) as text)
                            )
                            or (
                              (
                                cmp.ctry_group is null
                              ) and (
                                'China Selfcare' is null
                              )
                            )
                          )
                          then cast('RMB' as varchar)
                          when (
                            (
                              cast((
                                cmp.ctry_group
                              ) as text) = cast((
                                cast('China Personal Care' as varchar)
                              ) as text)
                            )
                            or (
                              (
                                cmp.ctry_group is null
                              ) and (
                                'China Personal Care' is null
                              )
                            )
                          )
                          then cast('RMB' as varchar)
                          else copa.obj_crncy_co_obj
                        end
                      ) as text)
                    )
                  )
                  then sum((
                    copa.amt_obj_crncy * exch_rate.ex_rt
                  ))
                  else cast((
                    cast((
                      0
                    ) as decimal)
                  ) as decimal(18, 0))
                end as gts_lcy,
                case
                  when (
                    (
                      cast((
                        copa.acct_hier_shrt_desc
                      ) as text) = cast((
                        cast('EQ' as varchar)
                      ) as text)
                    )
                    and (
                      cast((
                        exch_rate.to_crncy
                      ) as text) = cast((
                        cast('USD' as varchar)
                      ) as text)
                    )
                  )
                  then sum((
                    copa.amt_obj_crncy * exch_rate.ex_rt
                  ))
                  else cast((
                    cast((
                      0
                    ) as decimal)
                  ) as decimal(18, 0))
                end as eq_usd,
                case
                  when (
                    (
                      cast((
                        copa.acct_hier_shrt_desc
                      ) as text) = cast((
                        cast('EQ' as varchar)
                      ) as text)
                    )
                    and (
                      cast((
                        exch_rate.to_crncy
                      ) as text) = cast((
                        case
                          when (
                            (
                              cast((
                                cmp.ctry_group
                              ) as text) = cast((
                                cast('India' as varchar)
                              ) as text)
                            )
                            or (
                              (
                                cmp.ctry_group is null
                              ) and (
                                'India' is null
                              )
                            )
                          )
                          then cast('INR' as varchar)
                          when (
                            (
                              cast((
                                cmp.ctry_group
                              ) as text) = cast((
                                cast('Philippines' as varchar)
                              ) as text)
                            )
                            or (
                              (
                                cmp.ctry_group is null
                              ) and (
                                'Philippines' is null
                              )
                            )
                          )
                          then cast('PHP' as varchar)
                          when (
                            (
                              cast((
                                cmp.ctry_group
                              ) as text) = cast((
                                cast('China Selfcare' as varchar)
                              ) as text)
                            )
                            or (
                              (
                                cmp.ctry_group is null
                              ) and (
                                'China Selfcare' is null
                              )
                            )
                          )
                         
                          then cast('RMB' as varchar)
                          when (
                            (
                              cast((
                                cmp.ctry_group
                              ) as text) = cast((
                                cast('China Personal Care' as varchar)
                              ) as text)
                            )
                            or (
                              (
                                cmp.ctry_group is null
                              ) and (
                                'China Personal Care' is null
                              )
                            )
                          )
                          then cast('RMB' as varchar)
                          else copa.obj_crncy_co_obj
                        end
                      ) as text)
                    )
                  )
                  then sum((
                    copa.amt_obj_crncy * exch_rate.ex_rt
                  ))
                  else cast((
                    cast(null as decimal)
                  ) as decimal(18, 0))
                end as eq_lcy,
                case
                  when (
                    cast((
                      cmp.ctry_group
                    ) as text) = cast((
                      cast('India' as varchar)
                    ) as text)
                  )
                  then cast('INR' as varchar)
                  when (
                    cast((
                      cmp.ctry_group
                    ) as text) = cast((
                      cast('Philippines' as varchar)
                    ) as text)
                  )
                  then cast('PHP' as varchar)
                  when (
                    (
                      cast((
                        cmp.ctry_group
                      ) as text) = cast((
                        cast('China Selfcare' as varchar)
                      ) as text)
                    )
                    or (
                      cast((
                        cmp.ctry_group
                      ) as text) = cast((
                        cast('China Personal Care' as varchar)
                      ) as text)
                    )
                  )
                  then cast('RMB' as varchar)
                  else exch_rate.from_crncy
                end as from_crncy,
                exch_rate.to_crncy,
                case
                  when (
                    (
                      cast((
                        copa.acct_hier_shrt_desc
                      ) as text) = cast((
                        cast('NTS' as varchar)
                      ) as text)
                    )
                    and (
                      cast((
                        exch_rate.to_crncy
                      ) as text) = cast((
                        cast('USD' as varchar)
                      ) as text)
                    )
                  )
                  then sum(copa.qty)
                  else cast((
                    cast((
                      0
                    ) as decimal)
                  ) as decimal(18, 0))
                end as nts_qty,
                case
                  when (
                    (
                      cast((
                        copa.acct_hier_shrt_desc
                      ) as text) = cast((
                        cast('GTS' as varchar)
                      ) as text)
                    )
                    and (
                      cast((
                        exch_rate.to_crncy
                      ) as text) = cast((
                        cast('USD' as varchar)
                      ) as text)
                    )
                  )
                  then sum(copa.qty)
                  else cast((
                    cast((
                      0
                    ) as decimal)
                  ) as decimal(18, 0))
                end as gts_qty,
                case
                  when (
                    (
                      cast((
                        copa.acct_hier_shrt_desc
                      ) as text) = cast((
                        cast('EQ' as varchar)
                      ) as text)
                    )
                    and (
                      cast((
                        exch_rate.to_crncy
                      ) as text) = cast((
                        cast('USD' as varchar)
                      ) as text)
                    )
                  )
                  then sum(copa.qty)
                  else cast((
                    cast((
                      0
                    ) as decimal)
                  ) as decimal(18, 0))
                end as eq_qty,
                0 as numerator,
                0 as denominator
              from (
                (
                  (
                    edw_copa_trans_fact as copa
                      left join edw_company_dim as cmp
                        on (
                          (
                            cast((
                              copa.co_cd
                            ) as text) = cast((
                              cmp.co_cd
                            ) as text)
                          )
                        )
                  )
                  left join edw_material_dim as mat
                    on (
                      (
                        cast((
                          copa.matl_num
                        ) as text) = cast((
                          mat.matl_num
                        ) as text)
                      )
                    )
                )
                left join v_intrm_reg_crncy_exch_fiscper as exch_rate
                  on (
                    (
                      (
                        (
                          cast((
                            copa.obj_crncy_co_obj
                          ) as text) = cast((
                            exch_rate.from_crncy
                          ) as text)
                        )
                        and (
                          copa.fisc_yr_per = exch_rate.fisc_per
                        )
                      )
                      and case
                        when (
                          cast((
                            exch_rate.to_crncy
                          ) as text) <> cast((
                            cast('USD' as varchar)
                          ) as text)
                        )
                        then (
                          cast((
                            exch_rate.to_crncy
                          ) as text) = cast((
                            case
                              when (
                                (
                                  cast((
                                    cmp.ctry_group
                                  ) as text) = cast((
                                    cast('India' as varchar)
                                  ) as text)
                                )
                                or (
                                  (
                                    cmp.ctry_group is null
                                  ) and (
                                    'India' is null
                                  )
                                )
                              )
                              then cast('INR' as varchar)
                              when (
                                (
                                  cast((
                                    cmp.ctry_group
                                  ) as text) = cast((
                                    cast('Philippines' as varchar)
                                  ) as text)
                                )
                                or (
                                  (
                                    cmp.ctry_group is null
                                  ) and (
                                    'Philippines' is null
                                  )
                                )
                              )
                              then cast('php' as varchar)
                              when (
                                (
                                  cast((
                                    cmp.ctry_group
                                  ) as text) = cast((
                                    cast('China Selfcare' as varchar)
                                  ) as text)
                                )
                                or (
                                  (
                                    cmp.ctry_group is null
                                  ) and (
                                    'China Selfcare' is null
                                  )
                                )
                              )
                              then cast('RMB' as varchar)
                              when (
                                (
                                  cast((
                                    cmp.ctry_group
                                  ) as text) = cast((
                                    cast('China Personal Care' as varchar)
                                  ) as text)
                                )
                                or (
                                  (
                                    cmp.ctry_group is null
                                  ) and (
                                    'China Personal Care' is null
                                  )
                                )
                              )
                              then cast('RMB' as varchar)
                              else copa.obj_crncy_co_obj
                            end
                          ) as text)
                        )
                        else (
                          cast((
                            exch_rate.to_crncy
                          ) as text) = cast((
                            cast('USD' as varchar)
                          ) as text)
                        )
                      end
                    )
                  )
              )
              where
                (
                  (
                    (
                      (
                        cast((
                          copa.acct_hier_shrt_desc
                        ) as text) = cast((
                          cast('GTS' as varchar)
                        ) as text)
                      )
                      or (
                        cast((
                          copa.acct_hier_shrt_desc
                        ) as text) = cast((
                          cast('NTS' as varchar)
                        ) as text)
                      )
                    )
                    or (
                      cast((
                        copa.acct_hier_shrt_desc
                      ) as text) = cast((
                        cast('EQ' as varchar)
                      ) as text)
                    )
                  )
                  and (
                    cast((cast((copa.fisc_yr_per) as varchar))as text) >= (
                      (
                        (
                          cast(
                            (
                              date_part(year, current_timestamp())-2::varchar
                            ) as text) 
                          || 
                          cast(
                            (
                              0::varchar
                              ) 
                            as text)
                        ) || cast(
                            (
                              0::varchar
                              ) 
                            as text)
                      ) || cast(
                            (
                              1::varchar
                              ) 
                            as text)
                    )
                  )
                )
              group by
                1,
                copa.fisc_yr,
                copa.fisc_yr_per,
                copa.obj_crncy_co_obj,
                copa.matl_num,
                copa.co_cd,
                cmp.company_nm,
                copa.sls_org,
                copa.dstr_chnl,
                copa.div,
                copa.cust_num,
                copa.acct_num,
                copa.acct_hier_shrt_desc,
                exch_rate.from_crncy,
                exch_rate.to_crncy,
                cmp.ctry_group,
                cmp."CLUSTER",
                mat.mega_brnd_desc
            ) AS copa
            left join v_edw_customer_sales_dim as cust_sales
              on (
                (
                  (
                    (
                      (
                        cast((
                          copa.sls_org
                        ) as text) = cast((
                          cust_sales.sls_org
                        ) as text)
                      )
                      and (
                        cast((
                          copa.dstr_chnl
                        ) as text) = cast((
                          cust_sales.dstr_chnl
                        ) as text)
                      )
                    )
                    and (
                      cast((
                        copa.div
                      ) as text) = cast((
                        cust_sales.div
                      ) as text)
                    )
                  )
                  and (
                    cast((
                      copa.cust_num
                    ) as text) = cast((
                      cust_sales.cust_num
                    ) as text)
                  )
                )
              )
          )
          left join edw_vw_greenlight_skus as gn
            on (
              (
                (
                  (
                    ltrim(
                      cast((
                        copa.matl_num
                      ) as text),
                      cast((
                        cast((
                          0
                        ) as varchar)
                      ) as text)
                    ) = ltrim(gn.matl_num, cast((
                      cast((
                        0
                      ) as varchar)
                    ) as text))
                  )
                  and (
                    cast((
                      copa.sls_org
                    ) as text) = cast((
                      gn.sls_org
                    ) as text)
                  )
                )
                and (
                  cast((
                    copa.dstr_chnl
                  ) as text) = cast((
                    gn.dstr_chnl
                  ) as text)
                )
              )
            )
        )
        left join edw_code_descriptions_manual as code_desc
          on (
            (
              (
                cast((
                  code_desc.code
                ) as text) = cast((
                  cust_sales.segmt_key
                ) as text)
              )
              and (
                cast((
                  code_desc.code_type
                ) as text) = cast((
                  cast('Customer Segmentation Key' as varchar)
                ) as text)
              )
            )
          )
      )
      left join edw_customer_base_dim as cust_dim
        on (
          (
            cast((
              copa.cust_num
            ) as text) = cast((
              cust_dim.cust_num
            ) as text)
          )
        )
    )
    left join edw_sales_org_dim as sls_org_dim
      on (
        (
          cast((
            copa.sls_org
          ) as text) = cast((
            sls_org_dim.sls_org
          ) as text)
        )
      )
  )
  left join edw_dstrbtn_chnl as dist_chnl_dim
    on (
      (
        cast((
          copa.dstr_chnl
        ) as text) = cast((
          dist_chnl_dim.distr_chan
        ) as text)
      )
    )
)
where
  (
    (
      (
        (
          cast((
            copa.ctry_nm
          ) as text) <> cast((
            cast('OTC' as varchar)
          ) as text)
        )
        and (
          cast((
            copa.ctry_nm
          ) as text) <> cast((
            cast('India' as varchar)
          ) as text)
        )
      )
      and (
        cast((
          copa.ctry_nm
        ) as text) <> cast((
          cast('Japan' as varchar)
        ) as text)
      )
    )
    and (
      cast((
        copa.ctry_nm
      ) as text) <> cast((
        cast('APSC Regional' as varchar)
      ) as text)
    )
  )
group by
  copa."datasource",
  copa.fisc_yr,
  copa.fisc_yr_per,
  copa.fisc_day,
  copa.ctry_nm,
  copa.co_cd,
  copa.company_nm,
  copa.sls_org,
  sls_org_dim.sls_org_nm,
  copa.dstr_chnl,
  dist_chnl_dim.txtsh,
  copa."CLUSTER",
  copa.obj_crncy_co_obj,
  cust_sales."parent customer",
  cust_sales.banner,
  cust_sales."banner format",
  cust_sales.channel,
  cust_sales."go to model",
  cust_sales."sub channel",
  cust_sales.retail_env,
  copa.from_crncy,
  copa.to_crncy,
  copa.cust_num,
  cust_dim.cust_nm,
  cust_sales.segmt_key,
  code_desc.code_desc,
  gn.greenlight_sku_flag
UNION ALL
Select
  "map".dataset as "datasource",
  cast((
    otif.fiscal_yr
  ) as decimal(18, 0)) as fisc_yr,
  cast((
    (
      (
        to_char(
          cast(cast((
            to_date(
              (
                left(cast((
                  otif.fiscal_yr_mo
                ) as text), 4) || right(cast((
                  otif.fiscal_yr_mo
                ) as text), 2)
              ),
              cast((
                cast('YYYYMM' as varchar)
              ) as text)
            )
          ) as timestampntz) as timestampntz),
          cast((
            cast('YYYY' as varchar)
          ) as text)
        ) || cast((
          cast('0' as varchar)
        ) as text)
      ) || to_char(
        cast(cast((
          to_date(
            (
              left(cast((
                otif.fiscal_yr_mo
              ) as text), 4) || right(cast((
                otif.fiscal_yr_mo
              ) as text), 2)
            ),
            cast((
              cast('YYYYMM' as varchar)
            ) as text)
          )
        ) as timestampntz) as timestampntz),
        cast((
          cast('MM' as varchar)
        ) as text)
      )
    )
  ) as int) as fisc_yr_per,
  to_date(
    (
      (
        right(cast((
          otif.fiscal_yr_mo
        ) as text), 2) || cast((
          cast('01' as varchar)
        ) as text)
      ) || left(cast((
        otif.fiscal_yr_mo
      ) as text), 4)
    ),
    cast((
      cast('MMDDYYYY' as varchar)
    ) as text)
  ) as fisc_day,
  "map".destination_market as ctry_nm,
  inv.co_cd,
  cmp.company_nm,
  otif.salesorg as sls_org,
  sls_org_dim.sls_org_nm,
  inv.dstr_chnl,
  dist_chnl_dim.txtsh as dstr_chnl_nm,
  "map".destination_cluster AS "CLUSTER",
  cast(null as varchar) as obj_crncy_co_obj,
  cust_sales."parent customer",
  cust_sales.banner,
  cust_sales."banner format",
  cust_sales.channel,
  cust_sales."go to model",
  cust_sales."sub channel",
  cust_sales.retail_env,
  inv.cust_num,
  cust_dim.cust_nm as customer_name,
  case
    when (
      cast((
        inv.co_cd
      ) as text) = cast((
        cast('703A' as varchar)
      ) as text)
    )
    then cast('SE001' as varchar)
    else cust_sales.segmt_key
  end as segmt_key,
  case
    when (
      cast((
        inv.co_cd
      ) as text) = cast((
        cast('703A' as varchar)
      ) as text)
    )
    then cast('Lead' as varchar)
    else code_desc.code_desc
  end as segment,
  coalesce(gn.greenlight_sku_flag, cast('N/A' as varchar)) as greenlight_sku_flag,
  0 as nts_usd,
  0 as nts_lcy,
  0 as gts_usd,
  0 as gts_lcy,
  sum(otif.numerator_unit_otifd_delivery) as numerator,
  sum(otif.denom_unit_otifd) as denominator
from (
  (
    (
      (
        (
          (
            (
              (
                (
                  itg_otif_glbl_con_reporting AS otif
                    left join itg_mds_ap_sales_ops_map as "map"
                      on (
                        (
                          (
                            (
                              cast((
                                otif.country
                              ) as text) = cast((
                                "map".source_market
                              ) as text)
                            )
                            and (
                              cast((
                                otif.cluster_name
                              ) as text) = cast((
                                "map".source_cluster
                              ) as text)
                            )
                          )
                          and (
                            cast((
                              "map".dataset
                            ) as text) = cast((
                              cast('OTIF-D' as varchar)
                            ) as text)
                          )
                        )
                      )
                )
                Left join (
                  select distinct
                    edw_invoice_fact.co_cd,
                    edw_invoice_fact.dstr_chnl,
                    edw_invoice_fact.div,
                    edw_invoice_fact.sls_org,
                    edw_invoice_fact.sls_doc,
                    edw_invoice_fact.cust_num,
                    edw_invoice_fact.matl_num
                  from edw_invoice_fact
                ) as inv
                  on (
                    (
                      (
                        (
                          (
                            cast((
                              otif.doc_number
                            ) as text) = cast((
                              inv.sls_doc
                            ) as text)
                          )
                          and (
                            cast((
                              otif.salesorg
                            ) as text) = cast((
                              inv.sls_org
                            ) as text)
                          )
                        )
                        and (
                          cast((
                            otif.sold_to_nbr
                          ) as text) = cast((
                            inv.cust_num
                          ) as text)
                        )
                      )
                      and (
                        cast((
                          otif.material
                        ) as text) = cast((
                          inv.matl_num
                        ) as text)
                      )
                    )
                  )
              )
              left join edw_company_dim as cmp
                on (
                  (
                    cast((
                      inv.co_cd
                    ) as text) = cast((
                      cmp.co_cd
                    ) as text)
                  )
                )
            )
            left join edw_customer_base_dim as cust_dim
              on (
                (
                  cast((
                    inv.cust_num
                  ) as text) = cast((
                    cust_dim.cust_num
                  ) as text)
                )
              )
          )
          left join edw_sales_org_dim as sls_org_dim
            on (
              (
                cast((
                  otif.salesorg
                ) as text) = cast((
                  sls_org_dim.sls_org
                ) as text)
              )
            )
        )
        left join edw_dstrbtn_chnl as dist_chnl_dim
          on (
            (
              cast((
                inv.dstr_chnl
              ) as text) = cast((
                dist_chnl_dim.distr_chan
              ) as text)
            )
          )
      )
      left join edw_vw_greenlight_skus as gn
        on (
          (
            (
              (
                ltrim(
                  cast((
                    otif.material
                  ) as text),
                  cast((
                    cast((
                      0
                    ) as varchar)
                  ) as text)
                ) = ltrim(gn.matl_num, cast((
                  cast((
                    0
                  ) as varchar)
                ) as text))
              )
              and (
                cast((
                  otif.salesorg
                ) as text) = cast((
                  gn.sls_org
                ) as text)
              )
            )
            and (
              cast((
                inv.dstr_chnl
              ) as text) = cast((
                gn.dstr_chnl
              ) as text)
            )
          )
        )
    )
    left join v_edw_customer_sales_dim as cust_sales
      on (
        (
          (
            (
              (
                cast((
                  otif.salesorg
                ) as text) = cast((
                  cust_sales.sls_org
                ) as text)
              )
              and (
                cast((
                  otif.sold_to_nbr
                ) as text) = cast((
                  cust_sales.cust_num
                ) as text)
              )
            )
            and (
              cast((
                inv.dstr_chnl
              ) as text) = cast((
                cust_sales.dstr_chnl
              ) as text)
            )
          )
          and (
            cast((
              inv.div
            ) as text) = cast((
              cust_sales.div
            ) as text)
          )
        )
      )
  )
  left join edw_code_descriptions_manual as code_desc
    on (
      (
        (
          cast((
            code_desc.code
          ) as text) = cast((
            cust_sales.segmt_key
          ) as text)
        )
        and (
          cast((
            code_desc.code_type
          ) as text) = cast((
            cast('Customer Segmentation Key' as varchar)
          ) as text)
        )
      )
    )
)
where
  (
    (
      (
        (
          (
            (
              (
                (
                  (
                    (
                      cast((
                        otif."REGION"
                      ) as text) = cast((
                        cast('APAC' as varchar)
                      ) as text)
                    )
                    and (
                      cast((
                        otif.country
                      ) as text) <> cast((
                        cast('JP' as varchar)
                      ) as text)
                    )
                  )
                  and (
                    cast((
                      otif.no_charge_ind
                    ) as text) = cast((
                      cast('Revenue' as varchar)
                    ) as text)
                  )
                )
                and (
                  otif.denom_unit_otifd <> cast((
                    cast((
                      cast((
                        0
                      ) as decimal)
                    ) as decimal(18, 0))
                  ) as decimal(21, 2))
                )
              )
              and (
                cast((
                  otif.affiliate_flag
                ) as text) = cast((
                  cast('0' as varchar)
                ) as text)
              )
            )
            and (
              cast(

                  (date_part(year, current_timestamp())) - 3 :: varchar
               as text) < cast((
                otif.fiscal_yr
              ) as text)
            )
          )
          and (
            cast((
              "map".destination_market
            ) as text) <> cast((
              cast('OTC' as varchar)
            ) as text)
          )
        )
        and (
          cast((
            "map".destination_market
          ) as text) <> cast((
            cast('India' as varchar)
          ) as text)
        )
      )
      and (
        cast((
          "map".destination_market
        ) as text) <> cast((
          cast('Japan' as varchar)
        ) as text)
      )
    )
    and (
      cast((
        "map".destination_market
      ) as text) <> cast((
        cast('APSC Regional' as varchar)
      ) as text)
    )
  )
group by
  "map".dataset,
  cast((
    otif.fiscal_yr
  ) as decimal(18, 0)),
  cast((
    (
      (
        to_char(
          cast(cast((
            to_date(
              (
                left(cast((
                  otif.fiscal_yr_mo
                ) as text), 4) || right(cast((
                  otif.fiscal_yr_mo
                ) as text), 2)
              ),
              cast((
                cast('YYYYMM' as varchar)
              ) as text)
            )
          ) as timestampntz) as timestampntz),
          cast((
            cast('YYYY' as varchar)
          ) as text)
        ) || cast((
          cast('0' as varchar)
        ) as text)
      ) || to_char(
        cast(cast((
          to_date(
            (
              left(cast((
                otif.fiscal_yr_mo
              ) as text), 4) || right(cast((
                otif.fiscal_yr_mo
              ) as text), 2)
            ),
            cast((
              cast('YYYYMM' as varchar)
            ) as text)
          )
        ) as timestampntz) as timestampntz),
        cast((
          cast('MM' as varchar)
        ) as text)
      )
    )
  ) as int),
  to_date(
    (
      (
        right(cast((
          otif.fiscal_yr_mo
        ) as text), 2) || cast((
          cast('01' as varchar)
        ) as text)
      ) || left(cast((
        otif.fiscal_yr_mo
      ) as text), 4)
    ),
    cast((
      cast('MMDDYYYY' as varchar)
    ) as text)
  ),
  "map".destination_market,
  inv.co_cd,
  cmp.company_nm,
  otif.salesorg,
  sls_org_dim.sls_org_nm,
  inv.dstr_chnl,
  dist_chnl_dim.txtsh,
  "map".destination_cluster,
  13,
  cust_sales."parent customer",
  cust_sales.banner,
  cust_sales."banner format",
  cust_sales.channel,
  cust_sales."go to model",
  cust_sales."sub channel",
  cust_sales.retail_env,
  inv.cust_num,
  cust_dim.cust_nm,
  cust_sales.segmt_key,
  code_desc.code_desc,
  gn.greenlight_sku_flag
)

select * from final
