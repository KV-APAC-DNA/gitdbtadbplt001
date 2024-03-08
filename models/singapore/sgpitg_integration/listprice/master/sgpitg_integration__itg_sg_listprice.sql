{{
    config
    (
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["yearmo","mnth_type"]
    )
}}
with wks_sg_list_price as
(
    select * from {{ ref('sgpwks_integration__wks_sg_listprice') }}
),
edw_vw_sg_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
wks_sg_listprice_1 as

(
    select
            item_cd,
            MAX(snapshot_dt) as snapshot_dt
        from wks_sg_list_price
    where
    replace(left(SNAPSHOT_DT,7),'-','') = replace(left(dateadd(day,-1,CURRENT_TIMESTAMP()::DATE),7),'-','')
    group by
    item_cd
),
wks_sg_listprice_2 as
(
    select
        a.snapshot_dt,
        a.item_cd,
        MAX(valid_from) as valid_from,
        MAX(valid_to) as valid_to
    from wks_sg_list_price as a,
    wks_sg_listprice_1 as b
    where
    a.item_cd = b.item_cd and a.snapshot_dt = b.snapshot_dt
    group by
    a.snapshot_dt,
    a.item_cd
),
transformed_set1 as
(
    select
        b.plant as plant,
        b.cnty as cnty,
        b.item_cd as item_cd,
        b.item_desc as item_desc,
        b.valid_from as valid_from,
        b.valid_to as valid_to,
        b.rate as rate,
        b.currency as currency,
        b.price_unit as price_unit,
        b.uom as uom,
        replace(left(b.SNAPSHOT_DT,7),'-','') as yearmo,
        'CAL' as mnth_type,
        b.cdl_dttm as cdl_dttm,
        max(b.snapshot_dt) as snapshot_dt,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        cast(null as date) as updt_dttm
    from wks_sg_list_price as b,wks_sg_listprice_2 as c
    WHERE
    b.snapshot_dt = c.snapshot_dt
    and b.item_cd = c.item_cd
    and b.valid_to = c.valid_to
    and b.valid_from = c.valid_from
    and replace(left(b.SNAPSHOT_DT,7),'-','') = replace(left(dateadd(day,-1,CURRENT_TIMESTAMP()::DATE),7),'-','')
    group by
    b.plant,
    b.cnty,
    b.item_cd,
    b.item_desc,
    b.valid_from,
    b.valid_to,
    b.rate,
    b.currency,
    b.price_unit,
    b.uom,
    b.cdl_dttm,
    replace(left(b.SNAPSHOT_DT,7),'-','')
),

vw_time_dim_1 as
(
    select
        mnth_id as jj_mnth_id,
        cal_date
    from edw_vw_sg_time_dim
    group by
    mnth_id,
    cal_date
),
wks_sg_listprice_3 as
(
    select
        item_cd,
        veotd.jj_mnth_id,
        MAX(snapshot_dt) as snapshot_dt
    from wks_sg_list_price as c,
    vw_time_dim_1 as veotd
    where 
    c.snapshot_dt = veotd.cal_date
    and veotd.jj_mnth_id in 
    (
        select
        mnth_id
        from edw_vw_sg_time_dim as a
        where
        replace(dateadd(day,-1,CURRENT_TIMESTAMP()::DATE),'-','') = a.cal_date_id
    )
    group by
    item_cd,
    veotd.jj_mnth_id

),
wks_sg_listprice_4 as 
(
    select
        a.item_cd,
        a.snapshot_dt,
        jj_mnth_id,
        MAX(valid_to) as valid_to,
        MAX(valid_from) as valid_from
    from wks_sg_list_price as a,
    wks_sg_listprice_3 as b
    WHERE
    a.item_cd = b.item_cd and a.snapshot_dt = b.snapshot_dt
    group by
    a.item_cd,
    a.snapshot_dt,
    jj_mnth_id
    
),
transformed_set2 as 
(
    SELECT
        b.plant as plant,
        b.cnty as cnty,
        b.item_cd as item_cd,
        b.item_desc as item_desc,
        b.valid_from as valid_from,
        b.valid_to as valid_to,
        b.rate as rate,
        b.currency as currency,
        b.price_unit as price_unit,
        b.uom as uom,
        c.jj_mnth_id as yearmo,
        'JJ' as mnth_type,
        b.cdl_dttm as cdl_dttm,
        MAX(b.snapshot_dt) as snapshot_dt,
        current_timestamp() as crtd_dttm,
        cast(null as date) as updt_dttm
    from wks_sg_list_price as b,
    wks_sg_listprice_4 as c
    WHERE
    b.snapshot_dt = c.snapshot_dt
    and b.item_cd = c.item_cd
    and b.valid_to = c.valid_to
    and b.valid_from = c.valid_from
    group by
    b.plant,
    b.cnty,
    b.item_cd,
    b.item_desc,
    b.valid_from,
    b.valid_to,
    b.rate,
    b.currency,
    b.price_unit,
    b.uom,
    c.jj_mnth_id,
    b.cdl_dttm

),
union_all as 
(
    select * from transformed_set1
    union all
    select * from transformed_set2
),
final as
(
    select 
    plant::varchar(4) as plant,
	cnty::varchar(4) as cnty,
	item_cd::varchar(20) as item_cd,
	item_desc::varchar(100) as item_desc,
	valid_from::varchar(10) as valid_from,
	valid_to::varchar(10) as valid_to,
	rate::number(20,4) as rate,
	currency::varchar(4) as currency,
	price_unit::number(16,4) as price_unit,
	uom::varchar(6) as uom,
	yearmo::varchar(6) as yearmo,
	mnth_type::varchar(6) as mnth_type,
	cdl_dttm::varchar(255) as cdl_dttm,
	snapshot_dt::date as snapshot_dt,
	crtd_dttm::timestamp_ntz(9) as crtd_dttm,
	updt_dttm::timestamp_ntz(9) as updt_dttm
    from union_all
)
select * from final