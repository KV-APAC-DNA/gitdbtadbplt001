{{
    config(
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= ["sales_grp_cd","sls_org","cnd_type","matl_num","sold_to_cust_cd","vld_frm","vld_to"],
        merge_exclude_columns= ["crt_dttm"]
    )
}}
with sdl_pos_prc_condition_map as 
(
    select * from {{ ref('ntaitg_integration__vw_stg_sdl_pos_prc_condition_map') }}

),
final as
(
    select 
        src.sls_org::varchar(25) as sls_org,
        src.sales_grp_cd::varchar(18) as sales_grp_cd,
        src.cnd_type::varchar(25) as cnd_type,
        ltrim(src.matl_num, 0)::varchar(40) as matl_num,
        ltrim(src.sold_to_cust_cd, 0)::varchar(100) as sold_to_cust_cd,
        to_decimal(price, '999999999999D99')::number(18,0) as price,
        src.vld_frm::date as vld_frm,
        src.vld_to::date as vld_to,
        cond_curr::varchar(15) as cond_curr,
        doc_currcy::varchar(15) as doc_currcy,
        recordmode::varchar(1) as recordmode,
        ctry_cd::varchar(25) as ctry_cd,
        current_timestamp::timestamp_ntz(9) as crt_dttm,
        current_timestamp::timestamp_ntz(9) as  updt_dttm,
    FROM sdl_pos_prc_condition_map SRC
)
select * from final