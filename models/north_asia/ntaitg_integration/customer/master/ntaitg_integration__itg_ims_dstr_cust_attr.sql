{{
    config
    (
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} as itg_ims_dstr_cust_attr USING {{ ref('ntaitg_integration__sdl_tw_ims_dstr_std_customer') }} t2 WHERE itg_ims_dstr_cust_attr.dstr_cd = t2.distributor_code AND itg_ims_dstr_cust_attr.dstr_cust_cd = t2.distributor_cusotmer_code AND itg_ims_dstr_cust_attr.ctry_cd = 'TW';
        delete from {{this}} as itg_ims_dstr_cust_attr using {{ ref('ntawks_integration__wks_itg_hk_ims_dstr_cust_attr') }} as wks_itg_hk_ims_dstr_cust_attr where wks_itg_hk_ims_dstr_cust_attr.dstr_customer_code = itg_ims_dstr_cust_attr.dstr_cust_cd and wks_itg_hk_ims_dstr_cust_attr.ctry_cd = itg_ims_dstr_cust_attr.ctry_cd and wks_itg_hk_ims_dstr_cust_attr.dstr_code = itg_ims_dstr_cust_attr.dstr_cd and wks_itg_hk_ims_dstr_cust_attr.chng_flg = 'U';
        {% endif %}"
    )
}}
with source as (
    select * from  {{ ref('ntaitg_integration__sdl_tw_ims_dstr_std_customer') }}
),
tw_ims_distributor_ingestion_metadata as
(
    select * from {{ source('ntawks_integration', 'tw_ims_distributor_ingestion_metadata') }}
), --static
itg_tw_ims_dstr_customer_mapping as
(
    select * from ntaitg_integration.itg_tw_ims_dstr_customer_mapping
), --take as a source
wks_itg_hk_ims_dstr_cust_attr as (
    select * from {{ ref('ntawks_integration__wks_itg_hk_ims_dstr_cust_attr') }}
),
tw_trans as
(
    SELECT 
        cust.distributor_code AS dstr_cd,
        COALESCE(meta.dstr_nm, '#') AS dstr_nm,
        cust.distributor_cusotmer_code AS dstr_cust_cd,
        cust.distributor_customer_name AS dstr_cust_nm,
        cust.distributor_sales_area AS dstr_cust_area,
        NULL AS dstr_cust_clsn1,
        NULL AS dstr_cust_clsn2,
        NULL AS dstr_cust_clsn3,
        NULL AS dstr_cust_clsn4,
        NULL AS dstr_cust_clsn5,
        'TW' AS ctry_cd,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        cust.distributor_address,
        cust.distributor_telephone,
        cust.distributor_contact,
        map.store_type,
        map.hq
    FROM source cust
        LEFT OUTER JOIN itg_tw_ims_dstr_customer_mapping map ON LTRIM (cust.distributor_cusotmer_code, '0') = LTRIM (map.distributors_customer_code, '0')
        AND cust.distributor_code = map.distributor_code
        LEFT OUTER JOIN (
            SELECT DISTINCT ctry_cd,
                distributor_code,
                dstr_nm
            FROM tw_ims_distributor_ingestion_metadata
            WHERE subject_area = 'Customer'
                AND ctry_cd = 'TW'
        ) meta ON cust.distributor_code = meta.distributor_code
),
tw_final AS
(   
    select
    t1.dstr_cd::varchar(10) as dstr_cd,
	t1.dstr_nm::varchar(20) as dstr_nm,
	t1.dstr_cust_cd::varchar(50) as dstr_cust_cd,
	t1.dstr_cust_nm::varchar(50) as dstr_cust_nm,
	t1.dstr_cust_area::varchar(20) as dstr_cust_area,
	t1.dstr_cust_clsn1::varchar(100) as dstr_cust_clsn1,
	t1.dstr_cust_clsn2::varchar(100) as dstr_cust_clsn2,
	t1.dstr_cust_clsn3::varchar(100) as dstr_cust_clsn3,
	t1.dstr_cust_clsn4::varchar(100) as dstr_cust_clsn4,
	t1.dstr_cust_clsn5::varchar(100) as dstr_cust_clsn5,
	t1.ctry_cd::varchar(3) as ctry_cd,
	t1.crt_dttm::timestamp_ntz(9) as crt_dttm,
	t1.updt_dttm::timestamp_ntz(9) as updt_dttm,
	t1.distributor_address::varchar(255) as distributor_address,
	t1.distributor_telephone::varchar(255) as distributor_telephone,
	t1.distributor_contact::varchar(255) as distributor_contact,
    case when t1.store_type='TW'
    THEN t2.store_type 
    else t1.store_type
    end::varchar(255) as store_type,
    case when t1.ctry_cd='TW'
    THEN t2.hq 
    else t1.hq
    end::varchar(255) as hq
    from tw_trans t1
        left join itg_tw_ims_dstr_customer_mapping t2 
        ON LTRIM (t1.dstr_cust_cd, '0') = LTRIM (t2.distributors_customer_code, '0')
        AND t1.dstr_cd = t2.distributor_code
),
hk_final as (
    select dstr_code::varchar(10) as dstr_cd,
        dstr_name::varchar(20) as dstr_nm,
        dstr_customer_code::varchar(50) as dstr_cust_cd,
        dstr_customer_name::varchar(50) as dstr_cust_nm,
        dstr_customer_area::varchar(20) as dstr_cust_area,
        dstr_customer_clsn1::varchar(100) as dstr_cust_clsn1,
        dstr_customer_clsn2::varchar(100) as dstr_cust_clsn2,
        dstr_customer_clsn3::varchar(100) as dstr_cust_clsn3,
        dstr_customer_clsn4::varchar(100) as dstr_cust_clsn4,
        dstr_customer_clsn5::varchar(100) as dstr_cust_clsn5,
        ctry_cd::varchar(3) as ctry_cd,
        case when chng_flg = 'I' then convert_timezone('UTC', current_timestamp()) else tgt_crt_dttm end::timestamp_ntz(9) as crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm,
        null::varchar(255) as distributor_address,
        null::varchar(255) as distributor_telephone,
        null::varchar(255) as distributor_contact,
        null::varchar(255) as store_type,
        null::varchar(255) as hq
    from wks_itg_hk_ims_dstr_cust_attr
),
final as (
    select * from tw_final
    union all
    select * from hk_final
)
select * from final