with edw_material_plant_dim as(
    select * from {{ ref('aspedw_integration__edw_material_plant_dim') }}
),
edw_material_dim as(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_gch_producthierarchy as(
    select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
edw_material_sales_dim as(
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
edw_material_uom as(
    select * from {{ ref('aspedw_integration__edw_material_uom') }}
),
rempd as 
(
    select distinct
        edw_material_plant_dim.matl_num,
        cast('SG' as text) as cntry_key,
        edw_material_plant_dim.prft_ctr
    from edw_material_plant_dim
    where
        (
        (cast((edw_material_plant_dim.plnt) as text) = cast('2210' as text))
        or (cast((edw_material_plant_dim.plnt) as text) = cast('221A' as text))
        )
),
derived_table2 as(
    select
        matl_num,
        ean_num,
        dstr_chnl,
        max(launch_dt) as launch_dt
    from edw_material_sales_dim
    where
    (
        (cast((sls_org) as text) = cast('2210' as text))
        and (cast((ean_num) as text) <> cast('' as text))
    )
    group by
    matl_num,
    ean_num,
    dstr_chnl

),
a as(
    select
        matl_num,
        ean_num,
        dstr_chnl,
        launch_dt
    from derived_table2
    group by
    matl_num,
    ean_num,
    dstr_chnl,
    launch_dt
    
),

b as (
    select
        matl_num,
        max(cast((dstr_chnl) as text)) as chnl
    from edw_material_sales_dim
    where
    (
        (cast((sls_org) as text) = cast('2210' as text))
        and 
        (cast((ean_num) as text) <> cast('' as text))
    )
    group by
    matl_num
        
),

rmsd as (
    select 
        a.matl_num,
        a.ean_num,
        a.launch_dt
    from a,b
    where
        (
        (cast((a.matl_num) as text) = cast((b.matl_num) as text))
        and (cast((a.dstr_chnl) as text) = b.chnl)
        )
            
),

remu as (
    select
        material,
        unit,
        base_uom,
        record_mode,
        uomz1d,
        uomn1d,
        cdl_dttm,
        crtd_dttm,
        updt_dttm
    from edw_material_uom
    where
        (
            (cast((base_uom) as text) = cast('PC' as text))
            and (cast((unit) as text) = cast('CSE' as text))
        )
    
),
combined_joins_for_sg as(
    select distinct
    cast((rempd.cntry_key) as varchar(4)) as cntry_key,
    ltrim(cast((remd.matl_num) as text), cast('0' as text)) as sap_matl_num,
    remd.matl_desc as sap_mat_desc,
    rmsd.ean_num,
    remd.matl_type_cd as sap_mat_type_cd,
    remd.matl_type_desc as sap_mat_type_desc,
    remd.base_uom_cd as sap_base_uom_cd,
    remd.prch_uom_cd as sap_prchse_uom_cd,
    remd.prodh1 as sap_prod_sgmt_cd,
    remd.prodh1_txtmd as sap_prod_sgmt_desc,
    remd.prod_base as sap_base_prod_cd,
    remd.base_prod_desc as sap_base_prod_desc,
    remd.mega_brnd_cd as sap_mega_brnd_cd,
    remd.mega_brnd_desc as sap_mega_brnd_desc,
    remd.brnd_cd as sap_brnd_cd,
    remd.brnd_desc as sap_brnd_desc,
    remd.vrnt as sap_vrnt_cd,
    remd.varnt_desc as sap_vrnt_desc,
    remd.put_up as sap_put_up_cd,
    remd.put_up_desc as sap_put_up_desc,
    remd.prodh2 as sap_grp_frnchse_cd,
    remd.prodh2_txtmd as sap_grp_frnchse_desc,
    remd.prodh3 as sap_frnchse_cd,
    remd.prodh3_txtmd as sap_frnchse_desc,
    remd.prodh4 as sap_prod_frnchse_cd,
    remd.prodh4_txtmd as sap_prod_frnchse_desc,
    remd.prodh5 as sap_prod_mjr_cd,
    remd.prodh5_txtmd as sap_prod_mjr_desc,
    remd.prodh5 as sap_prod_mnr_cd,
    remd.prodh5_txtmd as sap_prod_mnr_desc,
    remd.prodh6 as sap_prod_hier_cd,
    remd.prodh6_txtmd as sap_prod_hier_desc,
    regph."region" as gph_region,
    regph.regional_franchise as gph_reg_frnchse,
    regph.regional_franchise_group as gph_reg_frnchse_grp,
    regph.gcph_franchise as gph_prod_frnchse,
    regph.gcph_brand as gph_prod_brnd,
    regph.gcph_subbrand as gph_prod_sub_brnd,
    regph.gcph_variant as gph_prod_vrnt,
    regph.gcph_needstate as gph_prod_needstate,
    regph.gcph_category as gph_prod_ctgry,
    regph.gcph_subcategory as gph_prod_subctgry,
    regph.gcph_segment as gph_prod_sgmnt,
    regph.gcph_subsegment as gph_prod_subsgmnt,
    regph.put_up_code as gph_prod_put_up_cd,
    regph.put_up_description as gph_prod_put_up_desc,
    regph.size as gph_prod_size,
    regph.unit_of_measure as gph_prod_size_uom,
    rmsd.launch_dt,
    cast(((remu.uomz1d / remu.uomn1d)) as varchar) as qty_shipper_pc,
    ltrim(cast((rempd.prft_ctr) as text), cast('0' as text)) as prft_ctr,
    ltrim(cast((remd.tot_shlf_lif) as text), cast('0' as text)) as shlf_life
    from rempd,
    (
        edw_material_dim as remd
        left join edw_gch_producthierarchy as regph
        on (
            ltrim(cast((remd.matl_num) as text), cast('0' as text)) 
            = ltrim(cast((regph.materialnumber) as text), cast('0' as text))
            )
        left join rmsd
        on (
            ltrim(cast((remd.matl_num) as text), cast('0' as text))
            = ltrim(cast((rmsd.matl_num) as text), cast('0' as text))
        )
        left join remu
        on (
                ltrim(cast((remd.matl_num) as text), cast('0' as text)) 
                = ltrim(cast((remu.material) as text), cast('0' as text))

            )
    )
    where 
    (
        (
            (
                ltrim(cast(( remd.matl_num) as text), cast('0' as text)) 
                = ltrim(cast((rempd.matl_num) as text), cast('0' as text))
            )
            and
            (
                cast((remd.prod_hier_cd) as text) <> cast('' as text)
            )
        )
        and 
        (
            (
                (
                    (
                        (
                            (
                                (
                                    (
                                        (cast((remd.matl_type_cd) as text) = cast('FERT' as text))
                                        or (cast((remd.matl_type_cd) as text) = cast('HALB' as text))
                                    )
                                    or
                                    (cast((remd.matl_type_cd) as text) = cast('ROH' as text))
                                )
                                or 
                                (
                                    cast((remd.matl_type_cd )as text) = cast('FER2' as text)
                                )
                            )
                            or (cast((remd.matl_type_cd) as text) = cast('PROM' as text))
                        )
                        or (cast((remd.matl_type_cd) as text) = cast('SAPR' as text))
                    )
                    or 
                    (
                        cast((remd.matl_type_cd) as text) = cast('DISP' as text)
                    )
                )
                or 
                (
                    cast((remd.matl_type_cd) as text) = cast('ERSA' as text)
                )
        )
        or (cast((remd.matl_type_cd) as text) = cast('Z001' as text))
        )
    )    
),
final as(
    SELECT
    sg.cntry_key,
    sg.sap_matl_num,
    sg.sap_mat_desc,
    sg.ean_num,
    sg.sap_mat_type_cd,
    sg.sap_mat_type_desc,
    sg.sap_base_uom_cd,
    sg.sap_prchse_uom_cd,
    sg.sap_prod_sgmt_cd,
    sg.sap_prod_sgmt_desc,
    sg.sap_base_prod_cd,
    sg.sap_base_prod_desc,
    sg.sap_mega_brnd_cd,
    sg.sap_mega_brnd_desc,
    sg.sap_brnd_cd,
    sg.sap_brnd_desc,
    sg.sap_vrnt_cd,
    sg.sap_vrnt_desc,
    sg.sap_put_up_cd,
    sg.sap_put_up_desc,
    sg.sap_grp_frnchse_cd,
    sg.sap_grp_frnchse_desc,
    sg.sap_frnchse_cd,
    sg.sap_frnchse_desc,
    sg.sap_prod_frnchse_cd,
    sg.sap_prod_frnchse_desc,
    sg.sap_prod_mjr_cd,
    sg.sap_prod_mjr_desc,
    sg.sap_prod_mnr_cd,
    sg.sap_prod_mnr_desc,
    sg.sap_prod_hier_cd,
    sg.sap_prod_hier_desc,
    sg.gph_region,
    sg.gph_reg_frnchse,
    sg.gph_reg_frnchse_grp,
    sg.gph_prod_frnchse,
    sg.gph_prod_brnd,
    sg.gph_prod_sub_brnd,
    sg.gph_prod_vrnt,
    sg.gph_prod_needstate,
    sg.gph_prod_ctgry,
    sg.gph_prod_subctgry,
    sg.gph_prod_sgmnt,
    sg.gph_prod_subsgmnt,
    sg.gph_prod_put_up_cd,
    sg.gph_prod_put_up_desc,
    sg.gph_prod_size,
    sg.gph_prod_size_uom,
    sg.launch_dt,
    sg.qty_shipper_pc,
    sg.prft_ctr,
    sg.shlf_life
    from combined_joins_for_sg as sg
)

select * from final