with edw_gch_customerhierarchy as(
    select * from {{ ref('aspedw_integration__edw_gch_customerhierarchy') }}
),
edw_customer_base_dim as(
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
edw_company_dim as(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_dstrbtn_chnl as(
    select * from {{ ref('aspedw_integration__edw_dstrbtn_chnl') }}
),
edw_sales_org_dim as(
    select * from {{ ref('aspedw_integration__edw_sales_org_dim') }}
),
edw_customer_sales_dim as(
    select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
),
edw_code_descriptions as(
    select * from {{ ref('aspedw_integration__edw_code_descriptions') }}
),
edw_subchnl_retail_env_mapping as(
    select * from {{ source('aspedw_integration', 'edw_subchnl_retail_env_mapping') }}
),
a as
(
    select cust_num,
        min
        (
            (
                case
                    when
                    (
                        ((cust_del_flag)::text = null::text)
                        or (
                                (cust_del_flag is null)
                                and (null  is null)
                            )
                    )
                    then 'O'::character varying
                    when 
                    (
                        ((cust_del_flag)::text = ''::text)
                        or (
                                (cust_del_flag is null) 
                                and ('' is null)
                            )
                    )
                    then 'O'::character varying
                    else cust_del_flag
                end
            )::text
        )   as cust_del_flag
    from edw_customer_sales_dim
    where (sls_org)::text = '2210'::text
    group by cust_num
),
b as
(
    select 
        distinct cust_num,
        "max"((dstr_chnl)::text) as dstr_chnl
    from  edw_customer_sales_dim
    where (sls_org)::text = '2210'::text
    group by cust_num
),
final as
(
    select 
        (ltrim((recbd.cust_num)::text, '0'::text))::character varying as sap_cust_id,
        recbd.cust_nm as sap_cust_nm,
        recsd.sls_org as sap_sls_org,
        recd.company as sap_cmp_id,
        recd.ctry_key as sap_cntry_cd,
        recd.ctry_nm as sap_cntry_nm,
        recbd.addr as sap_addr,
        recbd.rgn as sap_region,
        recbd.dstrc as sap_state_cd,
        recbd.city as sap_city,
        recbd.pstl_cd as sap_post_cd,
        recsd.dstr_chnl as sap_chnl_cd,
        redc.txtsh as sap_chnl_desc,
        recsd.sls_ofc as sap_sls_office_cd,
        recsd.sls_ofc_desc as sap_sls_office_desc,
        recsd.sls_grp as sap_sls_grp_cd,
        recsd.sls_grp_desc as sap_sls_grp_desc,
        recsd.crncy_key as sap_curr_cd,
        recsd.prnt_cust_key as sap_prnt_cust_key,
        cddes_pck.code_desc as sap_prnt_cust_desc,
        recsd.chnl_key as sap_cust_chnl_key,
        cddes_chnl.code_desc AS sap_cust_chnl_desc,
        recsd.sub_chnl_key AS sap_cust_sub_chnl_key,
        cddes_subchnl.code_desc AS sap_sub_chnl_desc,
        recsd.go_to_mdl_key AS sap_go_to_mdl_key,
        cddes_gtm.code_desc AS sap_go_to_mdl_desc,
        recsd.bnr_key AS sap_bnr_key,
        cddes_bnrkey.code_desc AS sap_bnr_desc,
        recsd.bnr_frmt_key AS sap_bnr_frmt_key,
        cddes_bnrfmt.code_desc AS sap_bnr_frmt_desc,
        subchnl_retail_env.retail_env,
        regch.gcgh_region AS gch_region,
        regch.gcgh_cluster AS gch_cluster,
        regch.gcgh_subcluster AS gch_subcluster,
        regch.gcgh_market AS gch_market,
        regch.gcch_retail_banner AS gch_retail_banner
    FROM
    edw_gch_customerhierarchy as regch,
    edw_customer_base_dim as recbd,
    edw_company_dim recd,
    edw_dstrbtn_chnl redc,
    edw_sales_org_dim resod,
    a,
    b,                
    edw_customer_sales_dim as recsd
    left join  edw_code_descriptions as cddes_pck
    on  (cddes_pck.code)::text = (recsd.prnt_cust_key)::text
    and (cddes_pck.code_type)::text = 'Parent Customer Key'::text 
    left join  edw_code_descriptions as cddes_bnrkey 
    on  (cddes_bnrkey.code)::text = (recsd.bnr_key)::text
    and  (cddes_bnrkey.code_type)::text = 'Banner Key'::text
    left join  edw_code_descriptions as cddes_bnrfmt 
    on  (cddes_bnrfmt.code)::text = (recsd.bnr_frmt_key)::text
    and (cddes_bnrfmt.code_type)::text = 'Banner Format Key'::text
    left join  edw_code_descriptions as cddes_chnl 
    on  (cddes_chnl.code)::text = (recsd.chnl_key)::text
    and (cddes_chnl.code_type)::text = 'Channel Key'::text
    left join  edw_code_descriptions as cddes_gtm 
    on  (cddes_gtm.code)::text = (recsd.go_to_mdl_key)::text
    and (cddes_gtm.code_type)::text = 'Go To Model Key'::text
    left join 
    (
        edw_code_descriptions as cddes_subchnl
        left join  edw_subchnl_retail_env_mapping as subchnl_retail_env 
        on  upper((subchnl_retail_env.sub_channel)::text) = upper((cddes_subchnl.code_desc)::text)
    )
    on (cddes_subchnl.code)::text = (recsd.sub_chnl_key)::text
    and (cddes_subchnl.code_type)::text = 'Sub Channel Key'::text
    where                
    ((regch.customer)::text = (recbd.cust_num)::text)
    and ((recsd.cust_num)::text = (recbd.cust_num)::text)
    and ((recsd.cust_num)::text = (b.cust_num)::text)
    and ((recsd.dstr_chnl)::text = (redc.distr_chan)::text)
    and ((recsd.dstr_chnl)::text = b.dstr_chnl)   
    and 
    (
        (
            case
                when 
                (
                    ((recsd.cust_del_flag)::text = NULL::text)
                    OR 
                    (
                        (recsd.cust_del_flag IS NULL)
                        and (NULL  IS NULL)
                    )
                ) 
                then 'O'::character varying
                when 
                (
                    ((recsd.cust_del_flag)::text = ''::text)
                    OR 
                    (
                        (recsd.cust_del_flag IS NULL)
                        and ('' IS NULL)
                    )
                ) 
                then 'O'::character varying
                ELSE recsd.cust_del_flag
            END
        )::text = a.cust_del_flag
    )
    and ((a.cust_num)::text = (recsd.cust_num)::text)
    and ((recsd.sls_org)::text = (resod.sls_org)::text)
    and ((resod.sls_org_co_cd)::text = (recd.co_cd)::text)
    and ((recsd.sls_org)::text = '2210'::text)
)
select * from final