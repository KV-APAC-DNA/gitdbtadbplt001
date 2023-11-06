with 

source as (

    select * from {{ source('arsadpprd001', 'itg_cbd_gt_customer') }}

),

renamed as (

    select
        dstrbtr_id,
        ar_cd,
        old_cust_id,
        ar_nm,
        ar_adres,
        tel_phn,
        fax,
        city,
        region,
        ar_typ_cd,
        sls_dist,
        sls_office,
        sls_grp,
        sls_emp,
        sls_nm,
        src_file,
        bill_no,
        bill_moo,
        bill_soi,
        bill_rd,
        bill_subdist,
        bill_dist,
        bill_prvnce,
        bill_zip_cd,
        modify_dt,
        routestep1,
        routestep2,
        routestep3,
        routestep4,
        routestep5,
        routestep6,
        routestep7,
        routestep8,
        routestep9,
        routestep10,
        actv_status,
        cust_type,
        dstrbtr_nm,
        dstrbtr_cost_lvl,
        dstrbtr_status,
        dstrbtr_region,
        dstrbtr_cntry,
        curnt_dist,
        dstrbtr_inv_day,
        dstrbtr_cd,
        dstrbtr_fee,
        grp_nm,
        ar_typ_grp,
        typ_grp_nm,
        sales_district,
        sls_dist_city,
        sls_dist_region,
        sls_dist_city_eng,
        sls_dist_blng_to_dstrbtr,
        region_desc,
        dist_nm,
        sub_dist_nm,
        pricelevel,
        salesareaname,
        branchcode,
        branchname,
        frequencyofvisit,
        store,
        re_nm,
        crt_dttm,
        updt_dttm

    from source

)

select * from renamed
