with source as (
    select * from dev_dna_core.snapjpnedw_integration.dm_so_store_list
),

result as (
    select
        mjr_pdt::varchar(256) as mjr_pdt,
	    ph_nm::varchar(256) as ph_nm,
	    item_cd::varchar(256) as item_cd,
	    item_nm_kn::varchar(256) as item_nm_kn,
	    str_cd::varchar(256) as str_cd,
	    lgl_nm_kn::varchar(256) as lgl_nm_kn,
	    adrs_knj1::varchar(256) as adrs_knj1,
	    tel_no::varchar(256) as tel_no,
	    pc::number(6,0) as pc
    from source
)

select * from result