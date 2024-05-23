{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        unique_key= ["year"],
        pre_hook = "delete from {{this}} where cast(year as integer) in (
        select cast(year as integer) from {{ source('idnsdl_raw', 'sdl_mds_id_lav_bp_sop_target') }});"
    )
}}
with source as 
(
    select * from {{ source('idnsdl_raw', 'sdl_mds_id_lav_bp_sop_target') }}
),
edw_time_dim as 
(
    select * from snapidnedw_integration.edw_time_dim
),
trans as 
(
    SELECT  
            t1.year,
            t1.Franchise,
            t1.brand_code as brand,
            t1.Variant,
            t1.sell_in_niv,
            t1.type,
            t2.jj_mnth_long,
            DECODE(
                jj_mnth_long,
                'January',
                january,
                'February',
                february,
                'March',
                march,
                'April',
                april,
                'May',
                may,
                'June',
                june,
                'July',
                july,
                'August',
                august,
                'September',
                september,
                'October',
                october,
                'November',
                november,
                'December',
                december
            ) AS val
        FROM source AS t1,
                    (
                        SELECT DISTINCT jj_mnth_long
                        FROM edw_time_dim
                    ) AS t2
),
transformed as 
(
    SELECT YEAR,
            Franchise,
            brand,
            Variant,
            jj_mnth_long,
            (
                CASE
                    WHEN sell_in_niv = 'BP'
                    AND TYPE = 'Quantity (Dz)' THEN cast(replace(val, ',', '.') as numeric(18, 4))
                    ELSE 0
                END
            ) AS BP_QTN,
            (
                CASE
                    WHEN sell_in_niv = 'BP'
                    AND TYPE = 'Value' THEN cast(replace(val, ',', '.') as numeric(18, 4))
                    ELSE 0
                END
            ) AS BP_VAL,
            (
                CASE
                    WHEN sell_in_niv = 'SnOP'
                    AND TYPE = 'Quantity (Dz)' THEN cast(replace(val, ',', '.') as numeric(18, 4))
                    ELSE 0
                END
            ) AS S_OP_QTN,
            (
                CASE
                    WHEN sell_in_niv = 'SnOP'
                    AND TYPE = 'Value' THEN cast(replace(val, ',', '.') as numeric(18, 4))
                    ELSE 0
                END
            ) AS S_OP_VAL
        FROM trans
),
final as 
(
    select
	year::varchar(10) as year,
	franchise::varchar(75) as franchise,
	brand::varchar(75) as brand,
	variant::varchar(75) as variant,
	jj_mnth_long::varchar(75) as jj_mnth_long,
    sum(bp_qtn)::number(18,4) as bp_qtn,
    sum(bp_val)::number(18,4) as bp_val,
    sum(s_op_qtn)::number(18,4) as s_op_qtn,
    sum(s_op_val)::number(18,4) as s_op_val
    from transformed
    group by year,
    franchise,
    brand,
    variant,
    jj_mnth_long
)
select * from final
