with edw_material_dim as(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_material_plant_dim as(
    select * from {{ ref('aspedw_integration__edw_material_plant_dim') }}
),
edw_material_sales_dim as(
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
edw_gch_producthierarchy as(
    select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
md as
(
    select 
        edw_material_dim.matl_num,
        edw_material_dim.matl_desc,
        edw_material_dim.crt_on,
        edw_material_dim.crt_by_nm,
        edw_material_dim.chg_dttm,
        edw_material_dim.chg_by_nm,
        edw_material_dim.maint_sts_cmplt_matl,
        edw_material_dim.maint_sts,
        edw_material_dim.fl_matl_del_clnt_lvl,
        edw_material_dim.matl_type_cd,
        edw_material_dim.indstr_sectr,
        edw_material_dim.matl_grp_cd,
        edw_material_dim.old_matl_num,
        edw_material_dim.base_uom_cd,
        edw_material_dim.prch_uom_cd,
        edw_material_dim.doc_num,
        edw_material_dim.doc_type,
        edw_material_dim.doc_vers,
        edw_material_dim.pg_fmt__doc,
        edw_material_dim.doc_chg_num,
        edw_material_dim.pg_num_doc,
        edw_material_dim.num_sht,
        edw_material_dim.prdtn_memo_txt,
        edw_material_dim.pg_fmt_prdtn_memo,
        edw_material_dim.size_dims_txt,
        edw_material_dim.bsc_matl,
        edw_material_dim.indstr_std_desc,
        edw_material_dim.mercia_plan,
        edw_material_dim.prchsng_val_key,
        edw_material_dim.grs_wt_meas,
        edw_material_dim.net_wt_meas,
        edw_material_dim.wt_uom_cd,
        edw_material_dim.vol_meas,
        edw_material_dim.vol_uom_cd,
        edw_material_dim.cntnr_rqr,
        edw_material_dim.strg_cond,
        edw_material_dim.temp_cond_ind,
        edw_material_dim.low_lvl_cd,
        edw_material_dim.trspn_grp,
        edw_material_dim.haz_matl_num,
        edw_material_dim.div,
        edw_material_dim.cmpt,
        edw_material_dim.ean_obsol,
        edw_material_dim.gr_prtd_qty,
        edw_material_dim.prcmt_rule,
        edw_material_dim.src_supl,
        edw_material_dim.seasn_cat,
        edw_material_dim.lbl_type_cd,
        edw_material_dim.lbl_form,
        edw_material_dim.deact,
        edw_material_dim.prmry_upc_cd,
        edw_material_dim.ean_cat,
        edw_material_dim.lgth_meas,
        edw_material_dim.wdth_meas,
        edw_material_dim.hght_meas,
        edw_material_dim.dim_uom_cd,
        edw_material_dim.prod_hier_cd,
        edw_material_dim.stk_tfr_chg_cost,
        edw_material_dim.cad_ind,
        edw_material_dim.qm_prcmt_act,
        edw_material_dim.allw_pkgng_wt,
        edw_material_dim.wt_unit,
        edw_material_dim.allw_pkgng_vol,
        edw_material_dim.vol_unit,
        edw_material_dim.exces_wt_tlrnc,
        edw_material_dim.exces_vol_tlrnc,
        edw_material_dim.var_prch_ord_unit,
        edw_material_dim.rvsn_lvl_asgn_matl,
        edw_material_dim.configurable_matl_ind,
        edw_material_dim.btch_mgmt_reqt_ind,
        edw_material_dim.pkgng_matl_type_cd,
        edw_material_dim.max_lvl_vol,
        edw_material_dim.stack_fact,
        edw_material_dim.pkgng_matl_grp,
        edw_material_dim.auth_grp,
        edw_material_dim.vld_from_dt,
        edw_material_dim.del_dt,
        edw_material_dim.seasn_yr,
        edw_material_dim.prc_bnd_cat,
        edw_material_dim.bill_of_matl,
        edw_material_dim.extrnl_matl_grp_txt,
        edw_material_dim.cross_plnt_cnfg_matl,
        edw_material_dim.matl_cat,
        edw_material_dim.matl_coprod_ind,
        edw_material_dim.fllp_matl_ind,
        edw_material_dim.prc_ref_matl,
        edw_material_dim.cros_plnt_matl_sts,
        edw_material_dim.cros_dstn_chn_matl_sts,
        edw_material_dim.cros_plnt_matl_sts_vld_dt,
        edw_material_dim.chn_matl_vld_from_dt,
        edw_material_dim.tax_clsn_matl,
        edw_material_dim.catlg_prfl,
        edw_material_dim.min_rmn_shlf_lif,
        edw_material_dim.tot_shlf_lif,
        edw_material_dim.strg_pct,
        edw_material_dim.cntnt_uom_cd,
        edw_material_dim.net_cntnt_meas,
        edw_material_dim.cmpr_prc_unit,
        edw_material_dim.isr_matl_grp,
        edw_material_dim.grs_cntnt_meas,
        edw_material_dim.qty_conv_meth,
        edw_material_dim.intrnl_obj_num,
        edw_material_dim.envmt_rlvnt,
        edw_material_dim.prod_allc_dtrmn_proc,
        edw_material_dim.prc_prfl_vrnt,
        edw_material_dim.matl_qual_disc,
        edw_material_dim.mfr_part_num,
        edw_material_dim.mfr_num,
        edw_material_dim.intrnl_inv_mgmt,
        edw_material_dim.mfr_part_prfl,
        edw_material_dim.meas_usg_unit,
        edw_material_dim.rollout_seasn,
        edw_material_dim.dngrs_goods_ind_prof,
        edw_material_dim.hi_viscous_ind,
        edw_material_dim.in_bulk_lqd_ind,
        edw_material_dim.lvl_explc_ser_num,
        edw_material_dim.pkgng_matl_clse_pkgng,
        edw_material_dim.appr_btch_rec_ind,
        edw_material_dim.ovrd_chg_num,
        edw_material_dim.matl_cmplt_lvl,
        edw_material_dim.per_ind_shlf_lif_expn_dt,
        edw_material_dim.rd_rule_sled,
        edw_material_dim.prod_cmpos_prtd_pkgng,
        edw_material_dim.genl_item_cat_grp,
        edw_material_dim.gn_matl_logl_vrnt,
        edw_material_dim.prod_base,
        edw_material_dim.vrnt,
        edw_material_dim.put_up,
        edw_material_dim.mega_brnd_cd,
        edw_material_dim.brnd_cd,
        edw_material_dim.tech,
        edw_material_dim.color,
        edw_material_dim.seasonality,
        edw_material_dim.mfg_src_cd,
        edw_material_dim.crt_dttm,
        edw_material_dim.updt_dttm,
        edw_material_dim.mega_brnd_desc,
        edw_material_dim.brnd_desc,
        edw_material_dim.varnt_desc,
        edw_material_dim.base_prod_desc,
        edw_material_dim.put_up_desc,
        edw_material_dim.prodh1,
        edw_material_dim.prodh1_txtmd,
        edw_material_dim.prodh2,
        edw_material_dim.prodh2_txtmd,
        edw_material_dim.prodh3,
        edw_material_dim.prodh3_txtmd,
        edw_material_dim.prodh4,
        edw_material_dim.prodh4_txtmd,
        edw_material_dim.prodh5,
        edw_material_dim.prodh5_txtmd,
        edw_material_dim.prodh6,
        edw_material_dim.prodh6_txtmd,
        edw_material_dim.matl_type_desc
    from edw_material_dim
    where (
            (
                cast(
                    (
                        edw_material_dim.prod_hier_cd
                    ) as text
                ) <> cast('' as text)
            )
            and (
                ltrim(
                    cast(
                        (
                            edw_material_dim.matl_num
                        ) as text
                    ),
                    cast(
                        (0) as text
                    )
                ) in (
                    select ltrim(
                            cast(
                                (
                                    a.matl_num
                                ) as text
                            ),
                            cast(
                                (0) as text
                            )
                        ) as matl_num
                    from (
                            edw_material_plant_dim as a
                            join (
                                select distinct ltrim(
                                        cast(
                                            (
                                                a.matl_num
                                            ) as text
                                        ),
                                        cast(
                                            (
                                                0
                                            ) as text
                                        )
                                    ) as matl_num
                                from edw_material_sales_dim as a
                                where (
                                        (
                                            (
                                                cast(
                                                    (
                                                        a.sls_org
                                                    ) as text
                                                ) = cast('2400' as text)
                                            )
                                            or (
                                                cast(
                                                    (
                                                        a.sls_org
                                                    ) as text
                                                ) = cast('2500' as text)
                                            )
                                        )
                                        and (
                                            not case
                                                when (
                                                    cast(
                                                        (
                                                            a.matl_num
                                                        ) as text
                                                    ) = cast('' as text)
                                                ) then cast(null as varchar)
                                                else a.matl_num
                                            end is null
                                        )
                                    )
                            ) as b on (
                                (
                                    ltrim(
                                        cast(
                                            (
                                                a.matl_num
                                            ) as text
                                        ),
                                        cast(
                                            (
                                                0
                                            ) as text
                                        )
                                    ) = ltrim(
                                        b.matl_num,
                                        cast(
                                            (
                                                0
                                            ) as text
                                        )
                                    )
                                )
                            )
                        )
                    where (
                            (
                                (
                                    (
                                        cast(
                                            (
                                                a.plnt
                                            ) as text
                                        ) = cast('2400' as text)
                                    )
                                    or (
                                        cast(
                                            (
                                                a.plnt
                                            ) as text
                                        ) = cast('2500' as text)
                                    )
                                )
                                or (
                                    cast(
                                        (
                                            a.plnt
                                        ) as text
                                    ) = cast('241a' as text)
                                )
                            )
                            and (
                                not case
                                    when (
                                        cast(
                                            (
                                                a.matl_num
                                            ) as text
                                        ) = cast('' as text)
                                    ) then cast(null as varchar)
                                    else a.matl_num
                                end is null
                            )
                        )
                )
            )
        )
                            
)
,
transformed as(
    select th.cntry_key,
        th.sap_matl_num,
        th.sap_mat_desc,
        th.ean_num,
        th.sap_mat_type_cd,
        th.sap_mat_type_desc,
        th.sap_base_uom_cd,
        th.sap_prchse_uom_cd,
        th.sap_prod_sgmt_cd,
        th.sap_prod_sgmt_desc,
        th.sap_base_prod_cd,
        th.sap_base_prod_desc,
        th.sap_mega_brnd_cd,
        th.sap_mega_brnd_desc,
        th.sap_brnd_cd,
        th.sap_brnd_desc,
        th.sap_vrnt_cd,
        th.sap_vrnt_desc,
        th.sap_put_up_cd,
        th.sap_put_up_desc,
        th.sap_grp_frnchse_cd,
        th.sap_grp_frnchse_desc,
        th.sap_frnchse_cd,
        th.sap_frnchse_desc,
        th.sap_prod_frnchse_cd,
        th.sap_prod_frnchse_desc,
        th.sap_prod_mjr_cd,
        th.sap_prod_mjr_desc,
        th.sap_prod_mnr_cd,
        th.sap_prod_mnr_desc,
        th.sap_prod_hier_cd,
        th.sap_prod_hier_desc,
        th.gph_region,
        th.gph_reg_frnchse,
        th.gph_reg_frnchse_grp,
        th.gph_prod_frnchse,
        th.gph_prod_brnd,
        th.gph_prod_sub_brnd,
        th.gph_prod_vrnt,
        th.gph_prod_needstate,
        th.gph_prod_ctgry,
        th.gph_prod_subctgry,
        th.gph_prod_sgmnt,
        th.gph_prod_subsgmnt,
        th.gph_prod_put_up_cd,
        th.gph_prod_put_up_desc,
        th.gph_prod_size,
        th.gph_prod_size_uom,
        th.launch_dt,
        th.qty_shipper_pc,
        th.prft_ctr,
        th.shlf_life
    from (
            select cast(
                    (cast('TH' as varchar)) as varchar(4)
                ) as cntry_key,
                ltrim(
                    cast((md.matl_num) as text),
                    cast((0) as text)
                ) as sap_matl_num,
                md.matl_desc as sap_mat_desc,
                sales_dim.ean_num,
                md.matl_type_cd as sap_mat_type_cd,
                md.matl_type_desc as sap_mat_type_desc,
                md.base_uom_cd as sap_base_uom_cd,
                md.prch_uom_cd as sap_prchse_uom_cd,
                md.prodh1 as sap_prod_sgmt_cd,
                md.prodh1_txtmd as sap_prod_sgmt_desc,
                md.prod_base as sap_base_prod_cd,
                md.base_prod_desc as sap_base_prod_desc,
                md.mega_brnd_cd as sap_mega_brnd_cd,
                md.mega_brnd_desc as sap_mega_brnd_desc,
                md.brnd_cd as sap_brnd_cd,
                md.brnd_desc as sap_brnd_desc,
                md.vrnt as sap_vrnt_cd,
                md.varnt_desc as sap_vrnt_desc,
                md.put_up as sap_put_up_cd,
                md.put_up_desc as sap_put_up_desc,
                md.prodh2 as sap_grp_frnchse_cd,
                md.prodh2_txtmd as sap_grp_frnchse_desc,
                md.prodh3 as sap_frnchse_cd,
                md.prodh3_txtmd as sap_frnchse_desc,
                md.prodh4 as sap_prod_frnchse_cd,
                md.prodh4_txtmd as sap_prod_frnchse_desc,
                md.prodh5 as sap_prod_mjr_cd,
                md.prodh5_txtmd as sap_prod_mjr_desc,
                md.prodh5 as sap_prod_mnr_cd,
                md.prodh5_txtmd as sap_prod_mnr_desc,
                md.prodh6 as sap_prod_hier_cd,
                md.prodh6_txtmd as sap_prod_hier_desc,
                gph."region" as gph_region,
                gph.regional_franchise as gph_reg_frnchse,
                gph.regional_franchise_group as gph_reg_frnchse_grp,
                gph.gcph_franchise as gph_prod_frnchse,
                gph.gcph_brand as gph_prod_brnd,
                gph.gcph_subbrand as gph_prod_sub_brnd,
                gph.gcph_variant as gph_prod_vrnt,
                gph.gcph_needstate as gph_prod_needstate,
                gph.gcph_category as gph_prod_ctgry,
                gph.gcph_subcategory as gph_prod_subctgry,
                gph.gcph_segment as gph_prod_sgmnt,
                gph.gcph_subsegment as gph_prod_subsgmnt,
                gph.put_up_code as gph_prod_put_up_cd,
                gph.put_up_description as gph_prod_put_up_desc,
                gph.size as gph_prod_size,
                gph.unit_of_measure as gph_prod_size_uom,
                sales_dim.launch_dt,
                cast(
                    (cast(null as varchar)) as varchar(100)
                ) as qty_shipper_pc,
                pd.prft_ctr,
                cast(
                    (
                        ltrim(
                            cast((md.tot_shlf_lif) as text),
                            cast((0) as text)
                        )
                    ) as varchar(100)
                ) as shlf_life
            from  md
            left join edw_gch_producthierarchy as gph on (
                (
                    ltrim(
                        cast(
                            (md.matl_num) as text
                        ),
                        cast((0) as text)
                    ) = ltrim(
                        cast(
                            (
                                gph.materialnumber
                            ) as text
                        ),
                        cast((0) as text)
                    )
                )
            )  
            left join (
                select distinct edw_material_plant_dim.matl_num,
                    edw_material_plant_dim.prft_ctr
                from edw_material_plant_dim
                where (
                        (
                            (
                                cast(
                                    (
                                        edw_material_plant_dim.plnt
                                    ) as text
                                ) = cast('2400' as text)
                            )
                            or (
                                cast(
                                    (
                                        edw_material_plant_dim.plnt
                                    ) as text
                                ) = cast('2500' as text)
                            )
                        )
                        or (
                            cast(
                                (
                                    edw_material_plant_dim.plnt
                                ) as text
                            ) = CAST('241A' as text)
                        )
                    )
            ) as pd on (
                (
                    (
                        ltrim(
                            cast(
                                (md.matl_num) as text
                            ),
                            cast((0) as text)
                        ) = ltrim(
                            cast(
                                (pd.matl_num) as text
                            ),
                            cast((0) as text)
                        )
                    )
                    and (
                        not case
                            when (
                                cast(
                                    (
                                        pd.matl_num
                                    ) as text
                                ) = cast('' as text)
                            ) then cast(null as varchar)
                            else pd.matl_num
                        end is null
                    )
                )
            )
        
            left join (
                select msd_dedup.matl_num,
                    msd_dedup.ean_num,
                    msd_dedup.sls_org,
                    msd_dedup.launch_dt
                from (
                        select distinct msd.matl_num,
                            msd.ean_num,
                            msd.sls_org,
                            msd.launch_dt,
                            row_number() over (
                                partition by msd.matl_num
                                order by msd.sls_org,
                                    msd.ean_num desc
                            ) as rnk
                        from (
                                select 
                                    row_number() over (
                                    partition by trim(ltrim(cast((a.matl_num) as text),cast('0' as text))),
                                    a.ean_num,
                                    a.sls_org
                                    order by a.launch_dt desc
                                    ) as rnum,
                                    trim(
                                        ltrim(
                                            cast(
                                                (a.matl_num) as text
                                            ),
                                            cast('0' as text)
                                        )
                                    ) as matl_num,
                                    trim(
                                        ltrim(
                                            cast(
                                                (a.ean_num) as text
                                            ),
                                            cast('0' as text)
                                        )
                                    ) as ean_num,
                                    a.sls_org,
                                    max(a.launch_dt) over (
                                        partition by a.matl_num,a.sls_org order by null rows between unbounded preceding and unbounded following
                                    ) as launch_dt
                                from edw_material_sales_dim as a
                                where (
                                        (
                                            (
                                                (
                                                    cast(
                                                        (
                                                            a.sls_org
                                                        ) as text
                                                    ) = cast('2400' as text)
                                                )
                                                or (
                                                    cast(
                                                        (
                                                            a.sls_org
                                                        ) as text
                                                    ) = cast('2500' as text)
                                                )
                                            )
                                            and (
                                                (
                                                    cast(
                                                        (
                                                            a.dstr_chnl
                                                        ) as text
                                                    ) <> cast('19' as text)
                                                )
                                                and (
                                                    cast(
                                                        (
                                                            a.dstr_chnl
                                                        ) as text
                                                    ) <> cast('15' as text)
                                                )
                                            )
                                        )
                                        and (
                                            not case
                                                when (
                                                    cast(
                                                        (
                                                            a.ean_num
                                                        ) as text
                                                    ) = cast('' as text)
                                                ) then cast(null as varchar)
                                                else a.ean_num
                                            end is null
                                        )
                                    )
                            ) as msd
                        where (
                                (msd.rnum = 1)
                                and (
                                    not case
                                        when (
                                            msd.ean_num = cast('' as text)
                                        ) then cast(null as text)
                                        else msd.ean_num
                                    end is null
                                )
                            )
                    ) as msd_dedup
                where (msd_dedup.rnk = 1)
            ) as sales_dim on (
                (
                    ltrim(
                        sales_dim.matl_num,
                        cast((0) as text)
                    ) = ltrim(
                        cast((md.matl_num) as text),
                        cast((0) as text)
                    )
                )
            )
                        
        ) as th
)
select * from transformed