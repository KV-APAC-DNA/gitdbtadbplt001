with itg_mds_ph_lav_customer as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_lav_customer') }}
),
itg_mds_ph_ref_parent_customer as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_ref_parent_customer') }}
),
edw_customer_base_dim as(
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
edw_customer_sales_dim as(
    select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
),
edw_company_dim as (
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_sales_org_dim as(
    select * from {{ ref('aspedw_integration__edw_sales_org_dim') }}
),
final as
(
    select ercd.co_cd,
        ercd.sls_org,
        ercd.dstr_chnl as dstrbtr_chnl,
        implc.cust_id,
        implc.cust_nm,
        ercd.hs_num_and_street as address,
        ercd.city,
        ercd.delv_plnt as delplant,
        ercd.rgn as region,
        ercd.dstrc as sdistrict,
        ercd.sls_ofc as soffice,
        ercd.sls_ofc_desc as soffice_desc,
        ercd.sls_grp as sgroup,
        ercd.sls_grp_desc as sgroup_desc,
        ercd.fst_phn_num as phone_no,
        ercd.fax_num as fax_no,
        implc.parent_cust_cd,
        implc.parent_cust_nm,
        implc.dstrbtr_grp_cd,
        implc.dstrbtr_grp_nm,
        imprpc.rpt_grp_1_desc,
        imprpc.rpt_grp_2_desc,
        implc.rpt_grp_3_desc,
        implc.rpt_grp_4_desc,
        implc.rpt_grp_5_desc,
        implc.rpt_grp_6_desc,
        implc.rpt_grp_7_desc,
        implc.channel_cd,
        implc.channel_desc,
        implc.sub_chnl_cd,
        implc.sub_chnl_desc,
        implc.region_cd,
        implc.region_nm,
        implc.province_cd,
        implc.province_nm,
        implc.mun_cd,
        implc.mun_nm,
        implc.rpt_grp_9_desc,
        imprpc.rpt_grp_11_desc,
        imprpc.rpt_grp_12_desc
    from
        (
            select *
            from itg_mds_ph_lav_customer
            where active = 'Y'
        ) as implc,
        (
            select *
            from itg_mds_ph_ref_parent_customer
            where active = 'Y'
        ) as imprpc,
        (
            select ecbd.*,
                ecsd.sls_org,
                ecd.co_cd,
                ecsd.dstr_chnl,
                ecsd.delv_plnt,
                ecsd.sls_ofc,
                ecsd.sls_ofc_desc,
                ecsd.sls_grp,
                ecsd.sls_grp_desc
            from edw_customer_base_dim as ecbd,
                edw_customer_sales_dim ecsd,
                edw_company_dim as ecd,
                edw_sales_org_dim as esod,
                (
                    select cust_num,
                        min(
                            decode(cust_del_flag, null, 'O', '', 'O', cust_del_flag)
                        ) as cust_del_flag
                    from edw_customer_sales_dim
                    where sls_org in ('2300')
                    group by cust_num
                ) ref
            where ecsd.sls_org in ('2300')
                and ecsd.sls_org = esod.sls_org
                and esod.sls_org_co_cd = ecd.co_cd
                and ecsd.cust_num = ecbd.cust_num
                and decode(
                    ecsd.cust_del_flag,
                    null,
                    'O',
                    '',
                    'O',
                    ECSD.cust_del_flag
                ) = ref.cust_del_flag
                and ref.cust_num = ecsd.cust_num
        ) as ercd
    where ltrim(implc.parent_cust_cd, '0') = ltrim(imprpc.parent_cust_cd(+), '0')
        and ltrim(implc.cust_id, '0') = ltrim(ercd.cust_num(+), '0')
)
select * from final