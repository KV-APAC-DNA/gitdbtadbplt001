with sdl_fin_sim_miscdata as
(
    select * from {{ source('indsdl_raw', 'sdl_fin_sim_miscdata') }}
),
final as
(
    select
        matl_num::varchar(250) as matl_num,
	    sku_desc::varchar(250) as sku_desc,
	    brand_combi::varchar(250) as brand_combi,
	    fisc_yr::varchar(250) as fisc_yr,
	    month::varchar(250) as month,
	    chnl_desc2::varchar(250) as chnl_desc2,
	    nature::varchar(250) as nature,
	    amt_obj_crncy::varchar(250) as amt_obj_crncy,
	    qty::varchar(250) as qty,
	    filename::varchar(100) as filename,
	    run_id::varchar(250) as run_id,
	    crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm
           
    from sdl_fin_sim_miscdata
)
select * from final