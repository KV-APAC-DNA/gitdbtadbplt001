{{
    config(
        materialized='view'
    )
}}

with itg_mds_my_customer_hierarchy as
(
    select * from {{ ref('mysitg_integration__itg_mds_my_customer_hierarchy') }}
),
itg_mds_sg_customer_hierarchy as
(
    select * from {{ ref('sgpitg_integration__itg_mds_sg_customer_hierarchy') }}
),
itg_vw_kr_mds_salesgrp_sapcust_map as
(
    select * from {{ ref('aspitg_integration__itg_vw_kr_mds_salesgrp_sapcust_map') }}
),
itg_mds_th_customer_hierarchy as
(
    select * from {{ ref('thaitg_integration__itg_mds_th_customer_hierarchy') }}
),
itg_mds_hk_customer_hierarchy as
(
    select * from {{ ref('ntaitg_integration__itg_mds_hk_customer_hierarchy') }}
),
itg_mds_tw_customer_hierarchy as
(
    select * from {{ ref('ntaitg_integration__itg_mds_tw_customer_hierarchy') }}
),
edw_customer_attr_hier_dim as
(
    select * from {{ ref('aspedw_integration__edw_customer_attr_hier_dim') }}
),
v_edw_customer_sales_dim as
(
    select * from {{ ref('aspedw_integration__v_edw_customer_sales_dim') }}
),
itg_mds_vn_customer_segmentation as
(
    select * from {{ ref('vnmitg_integration__itg_mds_vn_customer_segmentation') }}
),
itg_mds_ph_ref_parent_customer as
(
    select * from {{ ref('phlitg_integration__itg_mds_ph_ref_parent_customer') }}
),
itg_mds_ph_lav_customer as
(
    select * from {{ ref('phlitg_integration__itg_mds_ph_lav_customer') }}
),
itg_mds_in_customer_segmentation as
(
    select * from {{ ref('inditg_integration__itg_mds_in_customer_segmentation') }}
),
final as
(
    select ctry_nm,
        coalesce(Cust_num, 'Not Available') as cust_num,
        coalesce(loc_channel1, 'Not Available') as loc_channel1,
        coalesce(loc_channel2, 'Not Available') as loc_channel2,
        coalesce(loc_channel3, 'Not Available') as loc_channel3,
        coalesce(loc_cust1, 'Not Available') as loc_cust1,	  
            coalesce(loc_cust2, 'Not Available') as loc_cust2,
        coalesce(loc_cust3, 'Not Available') as loc_cust3,       
        coalesce(customer_segmentation, 'Not Available') as customer_segmentation,
        coalesce(local_cust_segmentation, 'Not Available') as local_cust_segmentation,       
        coalesce(local_cust_segmentation_2, 'Not Available') as local_cust_segmentation_2
    from
    (	   
    select distinct 'Malaysia' as ctry_nm,
        sold_to as cust_num,
        territory as loc_channel1,
        channel as loc_channel2,
        'Not Available' as loc_channel3,
        customer as loc_cust1,
        'Not Available' as loc_cust2,
        'Not Available' as loc_cust3,
        reg_group as customer_segmentation,
        reg_group as local_cust_segmentation,
        customer_segmentation_level_2_code as local_cust_segmentation_2
    from itg_mds_my_customer_hierarchy 
    union all 
    select distinct 'Singapore' as ctry_nm ,
        customer_number as cust_num,
        channel as loc_channel1,
        'Not Available' as loc_channel2,
        'Not Available' as loc_channel3,
        customer_group_code as loc_cust1,
        'Not Available' as loc_cust2,
        'Not Available' as loc_cust3,
        customer_segmentation_code as customer_segmentation,
        customer_segmentation_code as local_cust_segmentation,
        customer_segmentation_level_2_code as local_cust_segmentation_2
    from itg_mds_sg_customer_hierarchy 
    union all 
    select distinct 'Korea' as ctry_nm,
        cust_num,
        channel as loc_channel1,
        store_type as loc_channel2,
        sales_group_code as loc_channel3,
        sales_grp_nm as loc_cust1,
        cust_num as loc_cust2,
        cust_nm as loc_cust3,
        customer_segmentation_code as customer_segmentation,
        customer_segmentation_code as local_cust_segmentation,
        customer_segmentation_level_2_code as local_cust_segmentation_2
    from itg_vw_kr_mds_salesgrp_sapcust_map 
    union all
    select distinct 'Thailand' as ctry_nm ,
        sold_to as cust_num,
        channel as loc_channel1,
        re as loc_channel2,
        sub_re as loc_channel3,
        region as loc_cust1,
        customer_name as loc_cust2,
        sold_to as loc_cust3,
        segmentation as customer_segmentation,
        segmentation as local_cust_segmentation,
        customer_segmentation_level_2_code as local_cust_segmentation_2   
    from itg_mds_th_customer_hierarchy
    union all
    select distinct 'Hong Kong' as ctry_nm,
        customer_code as cust_num,
        channel1 as loc_channel1,
        channel2 as loc_channel2,
        channel3 as loc_channel3,
        customer_code as loc_cust1,
        customer_name as loc_cust2,
        parent_customer as loc_cust3,
        local_customer_segmentation_1 as customer_segmentation,
        local_customer_segmentation_1 as local_cust_segmentation,
        local_customer_segmentation_2 as local_cust_segmentation_2
    from itg_mds_hk_customer_hierarchy
    union all 
    select distinct 'Taiwan' as ctry_nm,
        custnum as cust_num,
        channel1 as loc_channel1,
        sales_office_code as loc_channel2,
        'Not Available' as loc_channel3,
        strategy_customer_hierachy_name as loc_cust1,
        sales_group_code as loc_cust2,
        'Not Available' as loc_cust3,
        local_customer_segmentation_1 as customer_segmentation,
        local_customer_segmentation_1 as local_cust_segmentation,
        local_customer_segmentation_2 as local_cust_segmentation_2
    from (SELECT distinct ca.strategy_customer_hierachy_name,
                        LTRIM(ca.sold_to_party) AS custnum,
                        tw_cust.local_customer_segmentation_1,
                        tw_cust.local_customer_segmentation_2,
                        tw_cust.channel1,
                        tw_cust.sales_office_code,
                        tw_cust.sales_group_code
                FROM edw_customer_attr_hier_dim ca
                LEFT JOIN itg_mds_tw_customer_hierarchy tw_cust ON ca.strategy_customer_hierachy_name = tw_Cust.customer_strategic_typeb
                WHERE cntry = 'Taiwan') 
    
    union all 
    select distinct 'Vietnam' as ctry_nm,
        ltrim(vn_cust.cust_num,'0') as cust_num,
        vn_cust.retail_env as loc_channel1,
        vn_cust."sub channel" as loc_channel2,
            vn_cust."go to model" as loc_channel3,
            vn_cust."parent customer" as loc_cust1,
            vn_cust.banner as loc_cust2,
            'Not Available' as loc_cust3,
            cust_seg1 as customer_segmentation,
            cust_seg1 as local_cust_segmentation,
            cust_seg2 as local_cust_segmentation_2
    from (SELECT ltrim(rc.cust_num,'0') as cust_num,retail_env,"sub channel","go to model", "parent customer",banner,
        vc.customer_segmentation_level_1_code as cust_seg1,
        vc.customer_segmentation_level_2_code as cust_seg2
        from
        (select sls_org,ltrim(cust_num,'0') as cust_num,
                                "parent customer",
                                banner,
                                "banner format",
                                channel,
                                "go to model",
                                "sub channel",
                                retail_env,
                                row_number() over( partition by sls_org,cust_num order by prnt_cust_key desc) rn 
        from v_edw_customer_sales_dim where sls_org in ('260S','260A'))rc 
                LEFT JOIN itg_mds_vn_customer_segmentation vc ON ltrim(rc.cust_num,'0') = vc.cust_num 
        where rn = 1)vn_cust
    union all 
    select distinct 'Philippines' as ctry_nm,
        ph_cust.cust_id as cust_num,
        ph_cust.channel as loc_channel1,
        ph_cust.rpt_grp_2_desc as loc_channel2,
        ph_cust.sub_chnl_desc as loc_channel3,
        ph_cust.rpt_grp_6_desc as loc_cust1,
        ph_cust.cust_id as loc_cust2,
        ph_cust.cust_nm as loc_cust3,
        customer_segmentation1 as customer_segmentation,
        customer_segmentation1 as local_cust_segmentation,
        customer_segmentation2 as local_cust_segmentation_2
    from (SELECT distinct rc.channel,
                        rc.rpt_grp_2_desc,
                        lc.sub_chnl_desc,
                        rc.customer_segmentation1,
                        rc.customer_segmentation2,
                        lc.rpt_grp_6_desc,
                        lc.cust_id,
                        lc.cust_nm
                FROM itg_mds_ph_ref_parent_customer rc
                LEFT JOIN itg_mds_ph_lav_customer lc ON rc.parent_cust_cd = lc.parent_cust_cd where rc.active = 'Y' and lc.active = 'Y') ph_cust) 
    union all
    select 'India' as ctry_nm,
            customer_code as cust_num,
            sap_channel as loc_channel1,
            customer_channel as loc_channel2,
            region as loc_channel3,
            upper("parent customer") as loc_cust1,
            upper(customer_name) as loc_cust2,
            customer_code as loc_cust3,
            customer_segmentation_level_1 as customer_segmentation,
            customer_segmentation_level_1 as local_cust_segmentation,
            customer_segmentation_level_2 as local_cust_segmentation_2
    from  itg_mds_in_customer_segmentation cust
    left join(select cust_num , "parent customer" from
    (select  cust_num , "parent customer" , row_number ()over( partition by cust_num order by prnt_cust_key desc) as rn from  v_edw_customer_sales_dim) where rn = 1) cust_sales
    on cust.customer_code = ltrim(cust_sales.cust_num,'0')   
)
select * from final