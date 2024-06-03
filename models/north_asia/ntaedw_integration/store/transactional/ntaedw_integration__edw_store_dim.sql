with edw_vw_store_dim as (
	select * from ref{{('ntaedw_integration__edw_vw_store_dim')}}
),
final as (
    select
        ctry_cd::varchar(5) as ctry_cd,
        dstr_cd::varchar(10) as dstr_cd,
        dstr_nm::varchar(100) as dstr_nm,
        store_cd::varchar(50) as store_cd,
        store_nm::varchar(100) as store_nm,
        store_class::varchar(3) as store_class,
        dstr_cust_area::varchar(20) as dstr_cust_area,
        dstr_cust_clsn1::varchar(100) as dstr_cust_clsn1,
        dstr_cust_clsn2::varchar(100) as dstr_cust_clsn2,
        dstr_cust_clsn3::varchar(100) as dstr_cust_clsn3,
        dstr_cust_clsn4::varchar(100) as dstr_cust_clsn4,
        dstr_cust_clsn5::varchar(100) as dstr_cust_clsn5,
        effctv_strt_dt::varchar(52) as effctv_strt_dt,
        effctv_end_dt::varchar(52) as effctv_end_dt,
        distributor_address::varchar(255) as distributor_address,
        distributor_telephone::varchar(255) as distributor_telephone,
        distributor_contact::varchar(255) as distributor_contact,
        store_type::varchar(255) as store_type,
        hq::varchar(255) as hq
    from edw_vw_store_dim
)
select * from final