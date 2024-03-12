--Import CTE
with source as (
    select * from {{ source('bwa_access', 'bwa_material_attribute') }}
),

--Logical CTE

--Final CTE
final as (
    select 
        nvl(material,'') as matl_num,
        base_uom,
        basic_matl,
        try_to_date(createdon,'YYYYMMDD') as createdon,
        division,
        eanupc,
        floor(gross_wt) as gross_wt,
        logsys,
        manufactor,
        manu_matnr,
        matl_cat,
        matl_group,
        matl_type,
        floor(net_weight) as net_weight,
        po_unit,
        prod_hier,
        rt_sups,
        size_dim,
        unit_dim,
        unit_of_wt,
        floor(volume) as volume,
        volumeunit,
        floor(height) as height,
        floor(lenght) as lenght,
        floor(width) as width,
        bic_zz_mvgr1,
        bic_zz_mvgr2,
        bic_zz_mvgr3,
        bic_zz_mvgr4,
        bic_zz_mvgr5,
        prodh1,
        prodh2,
        prodh3,
        prodh4,
        prodh5,
        prodh6,
        bic_zmerciapl,
        try_to_date(ch_on,'YYYYMMDD') as ch_on,
        createdby,
        try_to_date(datefrom,'yyyymmdd') as datefrom,
        del_flag,
        ean_numtyp,
        bic_yuomcnvf::number as bic_yuomcnvf,
        bic_ztragr,
        bic_zxchpf,
        bic_zpur_key,
        bic_zhaz_mat,
        bic_zstr_cond,
        bic_zmhdrz,
        bic_zmhdhb,
        bic_zqmpur,
        bic_zmstav,
        bic_zskutec,
        bic_zmjrtec,
        bic_zz_color::number as bic_zz_color,
        bic_zz_stack::number as bic_zz_stack,
        bic_ztaklv,
        extmatlgrp,
        bic_zvpsta,
        bic_zpstat,
        bic_zoldmatl,
        std_descr,
        bic_zferth,
        bic_zmsource,
        bic_zbramin,
        bic_zprodseg,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        null as filename
    from source
    where material <> 'NA'
    and _deleted_='F'
)

--Final select
select * from final