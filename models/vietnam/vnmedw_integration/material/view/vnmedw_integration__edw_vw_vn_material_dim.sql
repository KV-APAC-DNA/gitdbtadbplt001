with edw_material_dim as 
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_material_plant_dim as 
(
    select * from {{ ref('aspedw_integration__edw_material_plant_dim') }}

),
edw_gch_producthierarchy as
(
    select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
empd as 
(
    select distinct 
        edw_material_plant_dim.matl_num,
        'VN'::character varying AS cntry_key
    from edw_material_plant_dim
    where ((edw_material_plant_dim.plnt)::text = ('260S'::character varying)::text)
),
vn as 
(
        select distinct (empd.cntry_key)::character varying(4) as cntry_key,
            emd.matl_num as sap_matl_num,
            emd.matl_desc as sap_mat_desc,
            (null::character varying)::character varying(100) as ean_num,
            emd.matl_type_cd as sap_mat_type_cd,
            emd.matl_type_desc as sap_mat_type_desc,
            emd.base_uom_cd as sap_base_uom_cd,
            emd.prch_uom_cd as sap_prchse_uom_cd,
            emd.prodh1 as sap_prod_sgmt_cd,
            emd.prodh1_txtmd as sap_prod_sgmt_desc,
            emd.prod_base as sap_base_prod_cd,
            emd.base_prod_desc as sap_base_prod_desc,
            emd.mega_brnd_cd as sap_mega_brnd_cd,
            emd.mega_brnd_desc as sap_mega_brnd_desc,
            emd.brnd_cd as sap_brnd_cd,
            emd.brnd_desc as sap_brnd_desc,
            emd.vrnt as sap_vrnt_cd,
            emd.varnt_desc as sap_vrnt_desc,
            emd.put_up as sap_put_up_cd,
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
            (current_timestamp()::character varying)::timestamp without time zone as launch_dt,
            (null::character varying)::character varying(100) as qty_shipper_pc,
            (null::character varying)::character varying(100) as prft_ctr,
            (null::character varying)::character varying(100) as shlf_life
        from edw_material_dim emd,
            edw_gch_producthierarchy egph,
            empd
        where (((
                (
                    (emd.matl_num)::text = (egph.materialnumber)::text
                )
                        and ((emd.matl_num)::text = (empd.matl_num)::text)
                )
        and (
            (emd.prod_hier_cd)::text <> (''::character varying)::text
        )
    )
    and (
        (
            (
                (
                    (
                        (
                            (emd.matl_type_cd)::text = ('FERT'::character varying)::text
                        )
                        OR (
                            (emd.matl_type_cd)::text = ('HALB'::character varying)::text
                        )
                    )
                    OR (
                        (emd.matl_type_cd)::text = ('PROM'::character varying)::text
                    )
                )
                OR (
                    (emd.matl_type_cd)::text = ('SAPR'::character varying)::text
                )
            )
            OR (
                (emd.matl_type_cd)::text = ('ROH'::character varying)::text
            )
        )
        OR (
            (emd.matl_type_cd)::text = ('FER2'::character varying)::text
        )
    )
)
),
final as 
(
select 
    vn.cntry_key,
    vn.sap_matl_num,
    vn.sap_mat_desc,
    vn.ean_num,
    vn.sap_mat_type_cd,
    vn.sap_mat_type_desc,
    vn.sap_base_uom_cd,
    vn.sap_prchse_uom_cd,
    vn.sap_prod_sgmt_cd,
    vn.sap_prod_sgmt_desc,
    vn.sap_base_prod_cd,
    vn.sap_base_prod_desc,
    vn.sap_mega_brnd_cd,
    vn.sap_mega_brnd_desc,
    vn.sap_brnd_cd,
    vn.sap_brnd_desc,
    vn.sap_vrnt_cd,
    vn.sap_vrnt_desc,
    vn.sap_put_up_cd,
    vn.sap_put_up_desc,
    vn.sap_grp_frnchse_cd,
    vn.sap_grp_frnchse_desc,
    vn.sap_frnchse_cd,
    vn.sap_frnchse_desc,
    vn.sap_prod_frnchse_cd,
    vn.sap_prod_frnchse_desc,
    vn.sap_prod_mjr_cd,
    vn.sap_prod_mjr_desc,
    vn.sap_prod_mnr_cd,
    vn.sap_prod_mnr_desc,
    vn.sap_prod_hier_cd,
    vn.sap_prod_hier_desc,
    vn.gph_region,
    vn.gph_reg_frnchse,
    vn.gph_reg_frnchse_grp,
    vn.gph_prod_frnchse,
    vn.gph_prod_brnd,
    vn.gph_prod_sub_brnd,
    vn.gph_prod_vrnt,
    vn.gph_prod_needstate,
    vn.gph_prod_ctgry,
    vn.gph_prod_subctgry,
    vn.gph_prod_sgmnt,
    vn.gph_prod_subsgmnt,
    vn.gph_prod_put_up_cd,
    vn.gph_prod_put_up_desc,
    vn.gph_prod_size,
    vn.gph_prod_size_uom,
    vn.launch_dt,
    vn.qty_shipper_pc,
    vn.prft_ctr,
    vn.shlf_life
from vn
) 

select * from final