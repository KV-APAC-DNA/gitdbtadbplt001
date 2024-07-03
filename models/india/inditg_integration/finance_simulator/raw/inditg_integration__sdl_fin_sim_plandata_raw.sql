{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}
with source as (
    select * from {{ source('indsdl_raw', 'sdl_fin_sim_plandata')}}
),
final as
 (
    select
        matl_num::varchar(100) as matl_num,
	    fisc_yr::varchar(10) as fisc_yr,
	    month::varchar(10) as month,
	    chnl_desc2::varchar(20) as chnl_desc2,
	    nature::varchar(50) as nature,
	    amt_obj_crncy::number(38,5) as amt_obj_crncy,
	    qty::number(38,5) as qty,
	    plan::varchar(10) as plan,
	    filename::varchar(100) as filename,
	    run_id::varchar(150) as run_id,
	    crtd_dttm::timestamp_ntz(9) as crtd_dttm
    from source
    {% if is_incremental() %}
    --this filter will only be applied on an incremental run
    where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
     
 )
select * from final