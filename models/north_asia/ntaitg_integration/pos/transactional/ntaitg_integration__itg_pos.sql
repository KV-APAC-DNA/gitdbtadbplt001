{{
    config
    (
        materialized="incremental",
        incremental_strategy='append',
        pre_hook= ["{% if var('pos_job_to_execute') == 'tw_pos' %}
                    delete from {{this}} where (pos_dt,vend_prod_cd,src_sys_cd,ctry_cd) in
                    (
                        select distinct pos_dt,vend_prod_cd,src_sys_cd,ctry_cd from
                        (
                            select * from {{ ref('ntawks_integration__wks_itg_pos_poya') }}
                            union all
                            select * from {{ ref('ntawks_integration__wks_itg_pos_7eleven') }}
                            union all
                            select * from {{ ref('ntawks_integration__wks_itg_pos_ec') }}
                            union all
                            select * from {{ ref('ntawks_integration__wks_itg_pos_cosmed') }}
                        )
                        where chng_flg = 'U'
                    );
                    {% elif var('pos_job_to_execute') == 'kr_pos' %}
                    delete from {{this}} itg_pos where pos_dt || nvl(ean_num, '#') || nvl(str_cd, '#') || upper(nvl(vend_nm, '#')) in 
                    (
                        select distinct pos_dt || nvl(ean_num, '#') || nvl(str_cd, '#') || upper(nvl(vend_nm, '#')) from snapntawks_integration.wks_itg_pos_emart_ecvan_ssg
                    )
                    and upper(itg_pos.src_sys_cd) = 'EMART'
                    and upper(itg_pos.ctry_cd) = 'KR'
                    {% endif %}"
                    ,
                    "{% if var('pos_job_to_execute') == 'tw_pos' %}
                    delete from {{this}} where (pos_dt,vend_prod_cd,src_sys_cd,ctry_cd,str_cd) in
                    (
                        select distinct pos_dt,vend_prod_cd,src_sys_cd,ctry_cd,str_cd from
                        (
                            select * from {{ ref('ntawks_integration__wks_itg_pos_carrefour') }}
                            union all
                            select * from {{ ref('ntawks_integration__wks_itg_pos_rt_mart') }}
                            union all
                            select * from {{ ref('ntawks_integration__wks_itg_pos_px_civila') }}
                        )
                        where chng_flg = 'U'
                    );
                    {% elif var('pos_job_to_execute') == 'kr_pos' %}
                    delete from {{this}} itg_pos using ntawks_integration.wks_itg_pos
                    where wks_itg_pos.pos_dt = itg_pos.pos_dt
                        and wks_itg_pos.ean_num = itg_pos.ean_num
                        and wks_itg_pos.src_sys_cd = itg_pos.src_sys_cd
                        and wks_itg_pos.ctry_cd = itg_pos.ctry_cd
                        and wks_itg_pos.chng_flg = 'U'
                        and itg_pos.src_sys_cd not in ('Emart', 'Costco')
                        and wks_itg_pos.str_cd = itg_pos.str_cd
                    {% endif %}
                    "
                    ,
                    "{% if var('pos_job_to_execute') == 'tw_pos' %}
                    delete from {{this}} where (pos_dt,coalesce(vend_prod_cd,'#'),src_sys_cd,ctry_cd,str_cd) in
                    (
                        select distinct pos_dt,coalesce(vend_prod_cd,'#'),src_sys_cd,ctry_cd,str_cd from
                        (
                            select * from {{ ref('ntawks_integration__wks_itg_pos_watson_store') }}
                        )
                        where chng_flg = 'U'
                    )
                    {% elif var('pos_job_to_execute') == 'kr_pos' %}
                    delete from {{this}} where (pos_dt,ean_num,src_sys_cd,ctry_cd,str_cd) in
                    (
                        select distinct pos_dt,ean_num,src_sys_cd,ctry_cd,str_cd from
                        (
                            select * from ntawks_integration.wks_itg_pos_costco
                            union all
                            select * from ntawks_integration.wks_itg_pos_emart
                        )
                        where chng_flg = 'U'
                    );
                    {% endif %}   
                    "
                ]
    )
}}
with wks_itg_pos_7eleven as (
    select * from {{ ref('ntawks_integration__wks_itg_pos_7eleven') }}
),
wks_itg_pos_poya as (
    select * from {{ ref('ntawks_integration__wks_itg_pos_poya') }}
),
wks_itg_pos_ec as (
    select * from {{ ref('ntawks_integration__wks_itg_pos_ec') }}
),
wks_itg_pos_watson_store as (
    select * from {{ ref('ntawks_integration__wks_itg_pos_watson_store') }}
),
wks_itg_pos_carrefour as (
    select * from {{ ref('ntawks_integration__wks_itg_pos_carrefour') }}
),
wks_itg_pos_cosmed as (
    select * from {{ ref('ntawks_integration__wks_itg_pos_cosmed') }}
),
wks_itg_pos_rt_mart as (
    select * from {{ ref('ntawks_integration__wks_itg_pos_rt_mart') }}
),
wks_itg_pos_px_civila as (
    select * from {{ ref('ntawks_integration__wks_itg_pos_px_civila') }}
),
wks_itg_pos_costco as (
    select * from ntawks_integration.wks_itg_pos_costco
),
wks_itg_pos as (
    select * from ntawks_integration.wks_itg_pos
),
wks_itg_pos_emart as (
    select * from ntawks_integration.wks_itg_pos_emart
),
wks_itg_pos_emart_ecvan_ssg as (
      select * from snapntawks_integration.wks_itg_pos_emart_ecvan_ssg  
)
{% if var('pos_job_to_execute') == 'tw_pos' %}
,
final as 
(    
    SELECT 
        pos_dt::date as pos_dt,
        vend_cd::varchar(40) as vend_cd,
        vend_nm::varchar(100) as vend_nm,
        prod_nm::varchar(100) as prod_nm,
        vend_prod_cd::varchar(40) as vend_prod_cd,
        vend_prod_nm::varchar(600) as vend_prod_nm,
        brnd_nm::varchar(40) as brnd_nm,
        ean_num::varchar(100) as ean_num,
        str_cd::varchar(40) as str_cd,
        str_nm::varchar(100) as str_nm,
        sls_qty::number(18,0) as sls_qty,
        sls_amt::number(16,5) as sls_amt,
        unit_prc_amt::number(16,5) as unit_prc_amt,
        sls_excl_vat_amt::number(16,5) as sls_excl_vat_amt,
        stk_rtrn_amt::number(16,5) as stk_rtrn_amt,
        stk_recv_amt::number(16,5) as stk_recv_amt,
        avg_sell_qty::number(16,5) as avg_sell_qty,
        cast(cum_ship_qty as integer) as cum_ship_qty,
        cast(cum_rtrn_qty as integer) as cum_rtrn_qty,
        cast(web_ordr_takn_qty as integer) as web_ordr_takn_qty,
        cast(web_ordr_acpt_qty as integer) as web_ordr_acpt_qty,
        cast(dc_invnt_qty as integer) as dc_invnt_qty,
        cast(invnt_qty as integer) as invnt_qty,
        invnt_amt::number(16,5) as invnt_amt,
        cast(invnt_dt as date) as invnt_dt,
        serial_num::varchar(40) as serial_num,
        prod_delv_type::varchar(40) as prod_delv_type,
        prod_type::varchar(40) as prod_type,
        dept_cd::varchar(40) as dept_cd,
        dept_nm::varchar(100) as dept_nm,
        spec_1_desc::varchar(100) as spec_1_desc,
        spec_2_desc::varchar(100) as spec_2_desc,
        cat_big::varchar(100) as cat_big,
        cat_mid::varchar(40) as cat_mid,
        cat_small::varchar(40) as cat_small,
        dc_prod_cd::varchar(40) as dc_prod_cd,
        cust_dtls::varchar(100) as cust_dtls,
        dist_cd::varchar(40) as dist_cd,
        crncy_cd::varchar(10) as crncy_cd,
        src_txn_sts::varchar(40) as src_txn_sts,
        cast(src_seq_num as integer) as src_seq_num,
        src_sys_cd::varchar(30) as src_sys_cd,
        ctry_cd::varchar(10) as ctry_cd,
        null::varchar(35) as src_mesg_no,
        null::varchar(3) as src_mesg_code,
        null::varchar(3) as src_mesg_func_code,
        null::date as src_mesg_date,
        null::varchar(3) as src_sale_date_form,
        null::varchar(10) as src_send_code,
        null::varchar(13) as src_send_ean_code,
        null::varchar(30) as src_send_name,
        null::varchar(13) as src_recv_qual,
        null::varchar(10) as src_recv_ean_code,
        null::varchar(35) as src_recv_name,
        null::varchar(3) as src_part_qual,
        null::varchar(13) as src_part_ean_code,
        null::varchar(10) as src_part_id,
        null::varchar(30) as src_part_name,
        null::varchar(35) as src_sender_id,
        null::varchar(10) as src_recv_date,
        null::varchar(6) as src_recv_time,
        null::number(8,0) as src_file_size,
        null::varchar(128) as src_file_path,
        null::varchar(1) as src_lega_tran,
        null::varchar(10) as src_regi_date,
        null::number(6,0) as src_line_no,
        null::varchar(20) as src_instore_code,
        null::number(15,0) as src_mnth_sale_amnt,
        null::varchar(3) as src_qty_unit,
        null::number(10,0) as src_mnth_sale_qty,
        null::varchar(5) as unit_of_pkg_sales,
        null::date as doc_send_date,
        null::varchar(5) as unit_of_pkg_invt,
        null::varchar(6) as doc_fun,
        null::varchar(40) as doc_no,
        null::varchar(6) as doc_fun_cd,
        null::varchar(40) as buye_loc_cd,
        null::varchar(40) as vend_loc_cd,
        null::varchar(40) as provider_loc_cd,
        null::number(18,0) as comp_qty,
        null::varchar(5) as unit_of_pkg_comp,
        null::number(18,0) as order_qty,
        null::varchar(5) as unit_of_pkg_order,
        case
                when chng_flg = 'I' then current_timestamp
                else tgt_crt_dttm
            end as crt_dttm,
        current_timestamp AS UPD_DTTM
    FROM 
    (
        select * from wks_itg_pos_7eleven
        union all
        select * from wks_itg_pos_poya
        union all
        select * from wks_itg_pos_ec
        union all
        select * from wks_itg_pos_watson_store
        union all
        select * from wks_itg_pos_carrefour
        union all
        select * from wks_itg_pos_cosmed
        union all
        select * from wks_itg_pos_rt_mart
        union all
        select * from wks_itg_pos_px_civila
    )a
)
select * from final
{% elif var("pos_job_to_execute") == 'kr_pos' %}
,
costco as
(
    select 
        pos_dt,
        vend_cd,
        vend_nm,
        prod_nm,
        vend_prod_cd,
        vend_prod_nm,
        brnd_nm,
        ean_num,
        str_cd,
        str_nm,
        sls_qty,
        sls_amt,
        unit_prc_amt,
        sls_excl_vat_amt,
        stk_rtrn_amt,
        stk_recv_amt,
        avg_sell_qty,
        CAST(cum_ship_qty AS INTEGER) AS cum_ship_qty,
        CAST(cum_rtrn_qty AS INTEGER) AS cum_rtrn_qty,
        CAST(web_ordr_takn_qty AS INTEGER) AS web_ordr_takn_qty,
        CAST(web_ordr_acpt_qty AS INTEGER) AS web_ordr_acpt_qty,
        CAST(dc_invnt_qty AS INTEGER) AS dc_invnt_qty,
        invnt_qty,
        invnt_amt,
        invnt_dt,
        serial_num,
        prod_delv_type,
        prod_type,
        dept_cd,
        dept_nm,
        spec_1_desc,
        spec_2_desc,
        cat_big,
        cat_mid,
        cat_small,
        dc_prod_cd,
        cust_dtls,
        dist_cd,
        crncy_cd,
        src_txn_sts,
        CAST(src_seq_num AS INTEGER) AS src_seq_num,
        src_sys_cd,
        ctry_cd,
        src_mesg_no,
        src_mesg_code,
        src_mesg_func_code,
        CAST(src_mesg_date as date) as src_mesg_date,
        src_sale_date_form,
        src_send_code,
        src_send_ean_code,
        src_send_name,
        src_recv_qual,
        src_recv_ean_code,
        src_recv_name,
        src_part_qual,
        src_part_ean_code,
        src_part_id,
        src_part_name,
        src_sender_id,
        src_recv_date,
        src_recv_time,
        src_file_size,
        src_file_path,
        src_lega_tran,
        src_regi_date,
        src_line_no,
        src_instore_code,
        src_mnth_sale_amnt,
        src_qty_unit,
        src_mnth_sale_qty,
        SRC_unit_of_pkg_sales as unit_of_pkg_sales,
        SRC_doc_send_date as doc_send_date,
        SRC_unit_of_pkg_invt as unit_of_pkg_invt,
        SRC_doc_fun as doc_fun,
        SRC_doc_no as doc_no,
        SRC_doc_fun_cd as doc_fun_cd,
        SRC_buye_loc_cd as buye_loc_cd,
        SRC_vend_loc_cd as vend_loc_cd,
        SRC_provider_loc_cd as provider_loc_cd,
        SRC_comp_qty as comp_qty,
        SRC_unit_of_pkg_comp as unit_of_pkg_comp,
        SRC_order_qty as order_qty,
        SRC_unit_of_pkg_order as unit_of_pkg_order,
        CASE
            WHEN CHNG_FLG = 'I' THEN current_timestamp()
            ELSE TGT_CRT_DTTM
        END AS CRT_DTTM,
        current_timestamp() AS UPD_DTTM
    from wks_itg_pos_costco
),
allretailers as 
(
    select 
        pos_dt,
        vend_cd,
        vend_nm,
        prod_nm,
        vend_prod_cd,
        vend_prod_nm,
        brnd_nm,
        ean_num,
        str_cd,
        str_nm,
        sls_qty,
        sls_amt,
        unit_prc_amt,
        sls_excl_vat_amt,
        stk_rtrn_amt,
        stk_recv_amt,
        avg_sell_qty,
        CAST(cum_ship_qty AS INTEGER) AS cum_ship_qty,
        CAST(cum_rtrn_qty AS INTEGER) AS cum_rtrn_qty,
        CAST(web_ordr_takn_qty AS INTEGER) AS web_ordr_takn_qty,
        CAST(web_ordr_acpt_qty AS INTEGER) AS web_ordr_acpt_qty,
        CAST(dc_invnt_qty AS INTEGER) AS dc_invnt_qty,
        CAST(invnt_qty AS INTEGER) AS invnt_qty,
        invnt_amt,
        CAST(invnt_dt AS DATE) AS invnt_dt,
        serial_num,
        prod_delv_type,
        prod_type,
        dept_cd,
        dept_nm,
        spec_1_desc,
        spec_2_desc,
        cat_big,
        cat_mid,
        cat_small,
        dc_prod_cd,
        cust_dtls,
        dist_cd,
        crncy_cd,
        src_txn_sts,
        CAST(src_seq_num AS INTEGER) AS src_seq_num,
        src_sys_cd,
        ctry_cd,
        src_mesg_no,
        src_mesg_code,
        src_mesg_func_code,
        src_mesg_date,
        src_sale_date_form,
        src_send_code,
        src_send_ean_code,
        src_send_name,
        src_recv_qual,
        src_recv_ean_code,
        src_recv_name,
        src_part_qual,
        src_part_ean_code,
        src_part_id,
        src_part_name,
        src_sender_id,
        src_recv_date,
        src_recv_time,
        src_file_size,
        src_file_path,
        src_lega_tran,
        src_regi_date,
        src_line_no,
        src_instore_code,
        src_mnth_sale_amnt,
        src_qty_unit,
        src_mnth_sale_qty,
        unit_of_pkg_sales,
        CAST(doc_send_date AS DATE) AS doc_send_date,
        unit_of_pkg_invt,
        doc_fun,
        doc_no,
        doc_fun_cd,
        buye_loc_cd,
        vend_loc_cd,
        provider_loc_cd,
        comp_qty,
        unit_of_pkg_comp,
        order_qty,
        unit_of_pkg_order,
        CASE
            WHEN CHNG_FLG = 'I' THEN current_timestamp()
            ELSE TGT_CRT_DTTM
        END AS CRT_DTTM,
        current_timestamp() AS UPD_DTTM
    FROM wks_itg_pos
),
emart as 
(
    select 
        pos_dt,
        vend_cd,
        vend_nm,
        prod_nm,
        vend_prod_cd,
        vend_prod_nm,
        brnd_nm,
        ean_num,
        str_cd,
        str_nm,
        sls_qty,
        sls_amt,
        unit_prc_amt,
        sls_excl_vat_amt,
        stk_rtrn_amt,
        stk_recv_amt,
        avg_sell_qty,
        CAST(cum_ship_qty AS INTEGER) AS cum_ship_qty,
        CAST(cum_rtrn_qty AS INTEGER) AS cum_rtrn_qty,
        CAST(web_ordr_takn_qty AS INTEGER) AS web_ordr_takn_qty,
        CAST(web_ordr_acpt_qty AS INTEGER) AS web_ordr_acpt_qty,
        CAST(dc_invnt_qty AS INTEGER) AS dc_invnt_qty,
        CAST(invnt_qty AS INTEGER) AS invnt_qty,
        invnt_amt,
        CAST(invnt_dt AS DATE) AS invnt_dt,
        serial_num,
        prod_delv_type,
        prod_type,
        dept_cd,
        dept_nm,
        spec_1_desc,
        spec_2_desc,
        cat_big,
        cat_mid,
        cat_small,
        dc_prod_cd,
        cust_dtls,
        dist_cd,
        crncy_cd,
        src_txn_sts,
        CAST(src_seq_num AS INTEGER) AS src_seq_num,
        src_sys_cd,
        ctry_cd,
        src_mesg_no,
        src_mesg_code,
        src_mesg_func_code,
        src_mesg_date,
        src_sale_date_form,
        src_send_code,
        src_send_ean_code,
        src_send_name,
        src_recv_qual,
        src_recv_ean_code,
        src_recv_name,
        src_part_qual,
        src_part_ean_code,
        src_part_id,
        src_part_name,
        src_sender_id,
        src_recv_date,
        src_recv_time,
        src_file_size,
        src_file_path,
        src_lega_tran,
        src_regi_date,
        src_line_no,
        src_instore_code,
        src_mnth_sale_amnt,
        src_qty_unit,
        src_mnth_sale_qty,
        unit_of_pkg_sales,
        CAST(doc_send_date AS DATE) AS doc_send_date,
        unit_of_pkg_invt,
        doc_fun,
        doc_no,
        doc_fun_cd,
        buye_loc_cd,
        vend_loc_cd,
        provider_loc_cd,
        comp_qty,
        unit_of_pkg_comp,
        order_qty,
        unit_of_pkg_order,
        CASE
            WHEN CHNG_FLG = 'I' THEN current_timestamp()
            ELSE TGT_CRT_DTTM
        END AS CRT_DTTM,
        current_timestamp() AS UPD_DTTM
    FROM wks_itg_pos_emart
),
emart_ecvan_ssg as 
(
    select 
        pos_dt,
        vend_cd,
        vend_nm,
        prod_nm,
        vend_prod_cd,
        vend_prod_nm,
        brnd_nm,
        ean_num,
        str_cd,
        str_nm,
        sls_qty,
        sls_amt,
        unit_prc_amt,
        sls_excl_vat_amt,
        stk_rtrn_amt,
        stk_recv_amt,
        avg_sell_qty,
        CAST(cum_ship_qty AS INTEGER) AS cum_ship_qty,
        CAST(cum_rtrn_qty AS INTEGER) AS cum_rtrn_qty,
        CAST(web_ordr_takn_qty AS INTEGER) AS web_ordr_takn_qty,
        CAST(web_ordr_acpt_qty AS INTEGER) AS web_ordr_acpt_qty,
        CAST(dc_invnt_qty AS INTEGER) AS dc_invnt_qty,
        CAST(invnt_qty AS INTEGER) AS invnt_qty,
        invnt_amt,
        CAST(invnt_dt AS DATE) AS invnt_dt,
        serial_num,
        prod_delv_type,
        prod_type,
        dept_cd,
        dept_nm,
        spec_1_desc,
        spec_2_desc,
        cat_big,
        cat_mid,
        cat_small,
        dc_prod_cd,
        cust_dtls,
        dist_cd,
        crncy_cd,
        src_txn_sts,
        CAST(src_seq_num AS INTEGER) AS src_seq_num,
        src_sys_cd,
        ctry_cd,
        src_mesg_no,
        src_mesg_code,
        src_mesg_func_code,
        cast(src_mesg_date as date) as src_mesg_date,
        src_sale_date_form,
        src_send_code,
        src_send_ean_code,
        src_send_name,
        src_recv_qual,
        src_recv_ean_code,
        src_recv_name,
        src_part_qual,
        src_part_ean_code,
        src_part_id,
        src_part_name,
        src_sender_id,
        src_recv_date,
        src_recv_time,
        src_file_size,
        src_file_path,
        src_lega_tran,
        src_regi_date,
        src_line_no,
        src_instore_code,
        src_mnth_sale_amnt,
        src_qty_unit,
        src_mnth_sale_qty,
        unit_of_pkg_sales,
        CAST(doc_send_date AS DATE) AS doc_send_date,
        unit_of_pkg_invt,
        doc_fun,
        doc_no,
        doc_fun_cd,
        buye_loc_cd,
        vend_loc_cd,
        provider_loc_cd,
        comp_qty,
        unit_of_pkg_comp,
        order_qty,
        unit_of_pkg_order,
        TGT_CRT_DTTM AS CRT_DTTM,
        current_timestamp() AS UPD_DTTM
    from wks_itg_pos_emart_ecvan_ssg
),
emart_combined as 
(
    select * from emart_ecvan_ssg
    union all
    select * from emart 
    where pos_dt || nvl(ean_num, '#') || nvl(str_cd, '#') || upper(nvl(vend_nm, '#')) not in
    (
        select distinct pos_dt || nvl(ean_num, '#') || nvl(str_cd, '#') || upper(nvl(vend_nm, '#')) from emart_ecvan_ssg
    )
),
final as 
(
    select
        pos_dt::date as pos_dt,
        vend_cd::varchar(40) as vend_cd,
        vend_nm::varchar(100) as vend_nm,
        prod_nm::varchar(100) as prod_nm,
        vend_prod_cd::varchar(40) as vend_prod_cd,
        vend_prod_nm::varchar(600) as vend_prod_nm,
        brnd_nm::varchar(40) as brnd_nm,
        ean_num::varchar(100) as ean_num,
        str_cd::varchar(40) as str_cd,
        str_nm::varchar(100) as str_nm,
        sls_qty::number(18,0) as sls_qty,
        sls_amt::number(16,5) as sls_amt,
        unit_prc_amt::number(16,5) as unit_prc_amt,
        sls_excl_vat_amt::number(16,5) as sls_excl_vat_amt,
        stk_rtrn_amt::number(16,5) as stk_rtrn_amt,
        stk_recv_amt::number(16,5) as stk_recv_amt,
        avg_sell_qty::number(16,5) as avg_sell_qty,
        cum_ship_qty::number(18,0) as cum_ship_qty,
        cum_rtrn_qty::number(18,0) as cum_rtrn_qty,
        web_ordr_takn_qty::number(18,0) as web_ordr_takn_qty,
        web_ordr_acpt_qty::number(18,0) as web_ordr_acpt_qty,
        dc_invnt_qty::number(18,0) as dc_invnt_qty,
        invnt_qty::number(18,0) as invnt_qty,
        invnt_amt::number(16,5) as invnt_amt,
        invnt_dt::date as invnt_dt,
        serial_num::varchar(40) as serial_num,
        prod_delv_type::varchar(40) as prod_delv_type,
        prod_type::varchar(40) as prod_type,
        dept_cd::varchar(40) as dept_cd,
        dept_nm::varchar(100) as dept_nm,
        spec_1_desc::varchar(100) as spec_1_desc,
        spec_2_desc::varchar(100) as spec_2_desc,
        cat_big::varchar(100) as cat_big,
        cat_mid::varchar(40) as cat_mid,
        cat_small::varchar(40) as cat_small,
        dc_prod_cd::varchar(40) as dc_prod_cd,
        cust_dtls::varchar(100) as cust_dtls,
        dist_cd::varchar(40) as dist_cd,
        crncy_cd::varchar(10) as crncy_cd,
        src_txn_sts::varchar(40) as src_txn_sts,
        src_seq_num::number(18,0) as src_seq_num,
        src_sys_cd::varchar(30) as src_sys_cd,
        ctry_cd::varchar(10) as ctry_cd,
        src_mesg_no::varchar(35) as src_mesg_no,
        src_mesg_code::varchar(3) as src_mesg_code,
        src_mesg_func_code::varchar(3) as src_mesg_func_code,
        src_mesg_date::date as src_mesg_date,
        src_sale_date_form::varchar(3) as src_sale_date_form,
        src_send_code::varchar(10) as src_send_code,
        src_send_ean_code::varchar(13) as src_send_ean_code,
        src_send_name::varchar(30) as src_send_name,
        src_recv_qual::varchar(13) as src_recv_qual,
        src_recv_ean_code::varchar(10) as src_recv_ean_code,
        src_recv_name::varchar(35) as src_recv_name,
        src_part_qual::varchar(3) as src_part_qual,
        src_part_ean_code::varchar(13) as src_part_ean_code,
        src_part_id::varchar(10) as src_part_id,
        src_part_name::varchar(30) as src_part_name,
        src_sender_id::varchar(35) as src_sender_id,
        src_recv_date::varchar(10) as src_recv_date,
        src_recv_time::varchar(6) as src_recv_time,
        src_file_size::number(8,0) as src_file_size,
        src_file_path::varchar(128) as src_file_path,
        src_lega_tran::varchar(1) as src_lega_tran,
        src_regi_date::varchar(10) as src_regi_date,
        src_line_no::number(6,0) as src_line_no,
        src_instore_code::varchar(20) as src_instore_code,
        src_mnth_sale_amnt::number(15,0) as src_mnth_sale_amnt,
        src_qty_unit::varchar(3) as src_qty_unit,
        src_mnth_sale_qty::number(10,0) as src_mnth_sale_qty,
        unit_of_pkg_sales::varchar(5) as unit_of_pkg_sales,
        doc_send_date::date as doc_send_date,
        unit_of_pkg_invt::varchar(5) as unit_of_pkg_invt,
        doc_fun::varchar(6) as doc_fun,
        doc_no::varchar(40) as doc_no,
        doc_fun_cd::varchar(6) as doc_fun_cd,
        buye_loc_cd::varchar(40) as buye_loc_cd,
        vend_loc_cd::varchar(40) as vend_loc_cd,
        provider_loc_cd::varchar(40) as provider_loc_cd,
        comp_qty::number(18,0) as comp_qty,
        unit_of_pkg_comp::varchar(5) as unit_of_pkg_comp,
        order_qty::number(18,0) as order_qty,
        unit_of_pkg_order::varchar(5) as unit_of_pkg_order,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        upd_dttm::timestamp_ntz(9) as upd_dttm
    from
    (
        select * from costco
        union all
        select * from allretailers
        union all
        select * from emart_combined
    )   
)
select * from final
{% endif %}