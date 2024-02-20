{{
    config(
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["division","distr_chan","salesorg","cust_sales"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}

with 

source as (

    select * from {{ ref('aspwks_integration__wks_itg_cust_sls_attr') }}
),

final as (
    select
        division::varchar(2) as division,
        distr_chan::varchar(2) as distr_chan,
        salesorg::varchar(4) as salesorg,
        cust_sales::varchar(10) as cust_sales,
        accnt_asgn::varchar(2) as accnt_asgn,
        cust_cla::varchar(2) as cust_cla,
        cust_group::varchar(2) as cust_group,
        cust_grp1::varchar(3) as cust_grp1,
        cust_grp2::varchar(3) as cust_grp2,
        cust_grp3::varchar(3) as cust_grp3,
        cust_grp4::varchar(3) as cust_grp4,
        cust_grp5::varchar(3) as cust_grp5,
        c_ctr_area::varchar(4) as c_ctr_area,
        incoterms::varchar(3) as incoterms,
        incoterms2::varchar(32) as incoterms2,
        plant::varchar(4) as plant,
        pmnttrms::varchar(4) as pmnttrms,
        sales_dist::varchar(6) as sales_dist,
        sales_grp::varchar(3) as sales_grp,
        sales_off::varchar(4) as sales_off,
        cur_sls_emp::number(8,0) as cur_sls_emp,
        lcl_cust_grp_1::varchar(10) as lcl_cust_grp_1,
        lcl_cust_grp_2::varchar(10) as lcl_cust_grp_2,
        lcl_cust_grp_3::varchar(10) as lcl_cust_grp_3,
        lcl_cust_grp_4::varchar(10) as lcl_cust_grp_4,
        lcl_cust_grp_5::varchar(10) as lcl_cust_grp_5,
        lcl_cust_grp_6::varchar(10) as lcl_cust_grp_6,
        lcl_cust_grp_7::varchar(10) as lcl_cust_grp_7,
        lcl_cust_grp_8::varchar(10) as lcl_cust_grp_8,
        customer::varchar(10) as customer,
        prc_proc::varchar(1) as prc_proc,
        prc_grp::varchar(2) as prc_grp,
        prc_lst_type::varchar(2) as prc_lst_type,
        shpg_con::varchar(2) as shpg_con,
        par_del::varchar(1) as par_del,
        max_num_pa::varchar(1) as max_num_pa,
        prnt_cust_key::varchar(12) as prnt_cust_key,
        bnr_key::varchar(12) as bnr_key,
        bnr_frmt_key::varchar(12) as bnr_frmt_key,
        go_to_mdl_key::varchar(12) as go_to_mdl_key,
        chnl_key::varchar(12) as chnl_key,
        sub_chnl_key::varchar(12) as sub_chnl_key,
        segmt_key::varchar(12) as segmt_key,
        cust_set_1::varchar(12) as cust_set_1,
        cust_set_2::varchar(12) as cust_set_2,
        cust_set_3::varchar(12) as cust_set_3,
        cust_set_4::varchar(12) as cust_set_4,
        cust_set_5::varchar(12) as cust_set_5,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

select * from final