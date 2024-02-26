{{
    config(
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["lang_key","cntl_area","prft_ctr","vld_to_dt","vld_from_dt"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}

with 

source as (

    select * from {{ ref('aspwks_integration__wks_itg_prft_ctr_text') }}
),

trans as (
    select
        langu as lang_key,
        kokrs as cntl_area,
        prctr as prft_ctr,
        dateto as vld_to_dt,
        datefrom as vld_from_dt,
        txtsh as shrt_desc,
        txtmd as med_desc,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
),

final as(
    select
    lang_key::varchar(4) as lang_key,
	cntl_area::varchar(10) as cntl_area,
	prft_ctr::varchar(40) as prft_ctr,
	vld_to_dt::date as vld_to_dt,
	vld_from_dt::date as vld_from_dt,
	shrt_desc::varchar(20) as shrt_desc,
	med_desc::varchar(40) as med_desc,
	crt_dttm::timestamp_ntz(9) as crt_dttm,
	updt_dttm::timestamp_ntz(9) as updt_dttm
    from trans
)

select * from final