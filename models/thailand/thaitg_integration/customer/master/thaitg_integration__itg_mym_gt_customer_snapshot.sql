{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['snapshot_date_pk']
    )
}}

with source as(
    select * from {{ ref('thaitg_integration__itg_mym_gt_customer') }}
),
final as(
    select 
        DATEADD(day, -1, convert_timezone('Asia/Yangon', current_timestamp()))::timestamp_ntz(9) as snapshot_date,
        TO_CHAR(snapshot_date, 'YYYYMM') as snapshot_date_pk,
        dstrbtr_id::varchar(10) as dstrbtr_id,
        ar_cd::varchar(20) as ar_cd,
        old_cust_id::varchar(25) as old_cust_id,
        ar_nm::varchar(500) as ar_nm,
        ar_adres::varchar(500) as ar_adres,
        tel_phn::varchar(150) as tel_phn,
        fax::varchar(150) as fax,
        city::varchar(500) as city,
        region::varchar(20) as region,
        ar_typ_cd::varchar(20) as ar_typ_cd,
        sls_dist::varchar(10) as sls_dist,
        sls_office::varchar(30) as sls_office,
        sls_grp::varchar(30) as sls_grp,
        sls_emp::varchar(150) as sls_emp,
        sls_nm::varchar(250) as sls_nm,
        src_file::varchar(255) as src_file,
        bill_no::varchar(500) as bill_no,
        bill_moo::varchar(250) as bill_moo,
        bill_soi::varchar(255) as bill_soi,
        bill_rd::varchar(255) as bill_rd,
        bill_subdist::varchar(30) as bill_subdist,
        bill_dist::varchar(30) as bill_dist,
        bill_prvnce::varchar(30) as bill_prvnce,
        bill_zip_cd::varchar(50) as bill_zip_cd,
        modify_dt::timestamp_ntz(9) as modify_dt,
        routestep1::varchar(10) as routestep1,
        routestep2::varchar(10) as routestep2,
        routestep3::varchar(10) as routestep3,
        routestep4::varchar(10) as routestep4,
        routestep5::varchar(10) as routestep5,
        routestep6::varchar(10) as routestep6,
        routestep7::varchar(10) as routestep7,
        routestep8::varchar(10) as routestep8,
        routestep9::varchar(10) as routestep9,
        routestep10::varchar(10) as routestep10,
        actv_status::number(18,0) as actv_status,
        cust_type::varchar(50) as cust_type,
        dstrbtr_nm::varchar(100) as dstrbtr_nm,
        dstrbtr_cost_lvl::number(18,0) as dstrbtr_cost_lvl,
        dstrbtr_status::varchar(10) as dstrbtr_status,
        dstrbtr_region::varchar(20) as dstrbtr_region,
        dstrbtr_cntry::varchar(20) as dstrbtr_cntry,
        curnt_dist::varchar(10) as curnt_dist,
        dstrbtr_inv_day::number(3,0) as dstrbtr_inv_day,
        dstrbtr_cd::varchar(255) as dstrbtr_cd,
        dstrbtr_fee::float as dstrbtr_fee,
        grp_nm::varchar(100) as grp_nm,
        ar_typ_grp::varchar(10) as ar_typ_grp,
        typ_grp_nm::varchar(50) as typ_grp_nm,
        sales_district::varchar(50) as sales_district,
        sls_dist_city::varchar(100) as sls_dist_city,
        sls_dist_region::varchar(100) as sls_dist_region,
        sls_dist_city_eng::varchar(100) as sls_dist_city_eng,
        sls_dist_blng_to_dstrbtr::varchar(5) as sls_dist_blng_to_dstrbtr,
        region_desc::varchar(100) as region_desc,
        dist_nm::varchar(100) as dist_nm,
        sub_dist_nm::varchar(100) as sub_dist_nm,
        pricelevel::varchar(50) as pricelevel,
        salesareaname::varchar(150) as salesareaname,
        branchcode::varchar(50) as branchcode,
        branchname::varchar(150) as branchname,
        frequencyofvisit::varchar(50) as frequencyofvisit,
        store::varchar(200) as store,
        re_nm::varchar(100) as re_nm,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    FROM source
    WHERE
    CASE
        WHEN (select count(*) from source) > 0
        THEN 1
        ELSE 0
    END = 1

)
select * from final