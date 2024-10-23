{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        unique_keys=['cost_release_dt','vendor_no'],
        pre_hook = "delete from {{this}} where filename in (
        select distinct filename from {{ source('thasdl_raw', 'sdl_ecom_cogs') }} );"
    )
}}
with source as
(
    select * from {{ source('thasdl_raw', 'sdl_ecom_cogs') }}
),
final as
(
	select 
		plant::varchar(50) as plant,
		mat_type::varchar(50) as mat_type,
		proc_type::varchar(50) as proc_type,
		profit_center::varchar(100) as profit_center,
		profit_center_desc::varchar(255) as profit_center_desc,
		cost_release_dt::varchar(50) as cost_release_dt,
		cost_rel_yr::varchar(10) as cost_rel_yr,
		vendor_no::varchar(50) as vendor_no,
		vendor_name::varchar(255) as vendor_name,
		mat_group::varchar(50) as mat_group,
		material_grp_desc::varchar(100) as material_grp_desc,
		put_up::varchar(100) as put_up,
		put_up_desc::varchar(100) as put_up_desc,
		technology::varchar(50) as technology,
		brand::varchar(100) as brand,
		brand_desc::varchar(100) as brand_desc,
		site_id::varchar(100) as site_id,
		franchise::varchar(100) as franchise,
		franchise_desc::varchar(255) as franchise_desc,
		bol::varchar(50) as bol,
		material_no::varchar(50) as material_no,
		material_desc::varchar(255) as material_desc,
		uom::varchar(50) as uom,
		bom_qty::number(20,4) as bom_qty,
		scrap::number(20,4) as scrap,
		std_cost::number(20,4),
		unit::number(20,4) as std_cost,
		cur::varchar(50) as cur,
		cost_size::number(20,4) as cost_size,
		total_cost::number(20,4) as total_cost,
		raw_pack::number(20,4) as raw_pack,
		packaging::number(20,4) as packaging,
		labour::number(20,4) as labour,
		oh_direct::number(20,4) as oh_direct,
		oh_dir_var::number(20,4) as oh_dir_var,
		oh_dir_fix::number(20,4) as oh_dir_fix,
		oh_indirect::number(20,4) as oh_indirect,
		oh_ind_var::number(20,4) as oh_ind_var,
		oh_ind_fix::number(20,4) as oh_ind_fix,
		depreciation::number(20,4) as depreciation,
		sub_contract::number(20,4) as sub_contract,
		pfg_material::number(20,4) as pfg_material,
		freight::number(20,4) as freight,
		duty::number(20,4) as duty,
		other_pfg::number(20,4) as other_pfg,
		oh_subcontr::number(20,4) as oh_subcontr,
		oh_pfg::number(20,4) as oh_pfg,
		promot_cost::number(20,4) as promot_cost,
		promot_subc::number(20,4) as promot_subc,
		toll_fee_mk_up::number(20,4) as toll_fee_mk_up,
		ic_mark_up::number(20,4) as ic_mark_up,
		other_oh::number(20,4) as other_oh,
		other_oh_var::number(20,4) as other_oh_var,
		other_oh_fix::number(20,4) as other_oh_fix,
		final_cost::number(20,4) as final_cost,
		per_piece::number(20,4) as per_piece,
		per_dozen::number(20,4) as per_dozen,
		filename::varchar(255) as filename,
		crtd_dttm :: timestamp_ntz(9) as crtd_dttm,
		current_timestamp()::timestamp_ntz(9) as updt_dttm
	from source
)
select * from final