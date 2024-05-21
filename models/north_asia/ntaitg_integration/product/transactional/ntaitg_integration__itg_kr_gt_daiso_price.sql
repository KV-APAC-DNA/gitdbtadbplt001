delete from na_itg.itg_kr_gt_daiso_price
where (ean) in (
        select ean
        from na_sdl.sdl_kr_gt_daiso_price
    );
delete from na_itg.itg_kr_gt_daiso_price
where (ean) in (
        select trim(ean)
        from na_sdl.sdl_mds_kr_retailer_price_master
        where upper(retailer_code) = 'DAISO'
    );
with source_1 as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_gt_daiso_price') }}
),
source_2 as (
    select * from {{ source('ntasdl_raw', 'sdl_mds_kr_retailer_price_master') }}    
)
final as
(
    SELECT 
        'KR' as cntry_cd,
        dstr_nm,
        ean,
        unit_price,
        current_timestamp()
    from source_1
)
final as
(
    select 
        'KR' as cntry_cd,
        trim(retailer_code) as dstr_nm,
        trim(ean),
        unit_price,
        current_timestamp()
    from source_2
    where upper(retailer_code) = 'DAISO'
)
select * from final
