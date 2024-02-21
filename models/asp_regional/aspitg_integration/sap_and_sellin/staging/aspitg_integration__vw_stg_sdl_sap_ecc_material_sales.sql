with 

source as (

    select * from {{ source('bwa_access', 'bwa_material_sales') }}

),

final as (
    select 
    coalesce(salesorg,'') as salesorg,
	coalesce(distr_chan,'') as distr_chan,
	coalesce(mat_sales,'') as mat_sales,
	coalesce(base_uom,'') as base_uom,
	coalesce(matl_grp_1,'') as matl_grp_1,
	coalesce(prod_hier,'') as prod_hier,
	coalesce(prov_group,'') as prov_group,
	coalesce(rebate_grp,'') as rebate_grp,
	coalesce(ph_refnr,'') as ph_refnr,
	coalesce(del_flag,'') as del_flag,
	coalesce(matl_grp_2,'') as matl_grp_2,
	coalesce(matl_grp_3,'') as matl_grp_3,
	coalesce(matl_grp_4,'') as matl_grp_4,
	coalesce(matl_grp_5,'') as matl_grp_5,
	coalesce(mat_stgrp,'') as mat_stgrp,
	coalesce(rt_assgrad,'') as rt_assgrad,
	coalesce(af_vasmg,'') as af_vasmg,
	coalesce(af_prind,'') as af_prind,
	coalesce(bic_zpredecsr,'') as zpredecsr,
	coalesce(bic_zskuid,'') as zskuid,
	coalesce(bic_zprallocd,'') as zprallocd,
	coalesce(bic_zpackpcs,'') as zpackpcs,
	coalesce(bic_zean,'') as zean,
	coalesce(bic_zoldmatl,'') as zoldmatl,
	coalesce(bic_zd_plant,'') as zd_plant,
	coalesce(bic_zchdcind,'') as zchdcind,
	coalesce(bic_zprc_grp,'') as zprc_grp,
	coalesce(accnt_asgn,'') as accnt_asgn,
	coalesce(bic_zitm_cgrp,'') as zitm_cgrp,
	to_number(bic_zmin_qty,38,3) as zmin_qty,
	to_number(bic_zmin_dqty,38,3) as zmin_dqty,
	to_number(bic_zdel_unit,38,3) as zdel_unit,
	coalesce(bic_zschme,'') as zschme,
	coalesce(bic_zvrkme,'') as zvrkme,
	coalesce(bic_zlaunchd,'') as zlaunchd,
	coalesce(bic_znpi_ind,'') as znpi_ind,
	coalesce(bic_zlmat_gr1,'') as zlmat_gr1,
	coalesce(bic_zlmat_gr2,'') as zlmat_gr2,
	coalesce(bic_zlmat_gr3,'') as zlmat_gr3,
	coalesce(bic_zlmat_gr4,'') as zlmat_gr4,
	coalesce(bic_zlmat_gr5,'') as zlmat_gr5,
	coalesce(bic_zlmat_gr6,'') as zlmat_gr6,
	coalesce(bic_znpi_indr,'') as znpi_indr,
	coalesce(bic_zcpy_hist,'') as zcpy_hist,
	coalesce(bic_zabc,'') as zabc,
	coalesce(bic_zfc_indc,'') as zfc_indc,
	coalesce(bic_zprdtype,'') as zprdtype,
	coalesce(bic_zparent,'') as zparent,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
    where _deleted_='F'

)

select * from final
