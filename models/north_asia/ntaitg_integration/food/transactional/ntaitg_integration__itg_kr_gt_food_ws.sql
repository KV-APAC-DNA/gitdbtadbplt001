{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= " {% if is_incremental() %}
                    delete from na_itg.itg_kr_gt_food_ws where (
                    customer_code,
                    coalesce(sub_customer_code, 'NA')
                    ) in (
                    select cast(customer_code as varchar) as customer_code,
                        'NA' as sub_customer_code
                    from na_sdl.sdl_mds_kr_sub_customer_master
                    where upper(trim(retailer_code)) = 'FOOD_WS'
                    );
                    {% endif %}
                    "
    )
}}
with SDL_MDS_KR_SUB_CUSTOMER_MASTER as (
    select * from DEV_DNA_LOAD.SNAPNTASDL_RAW.SDL_MDS_KR_SUB_CUSTOMER_MASTER
),
transformed as (
SELECT 'KR' as cntry_cd,
    RETAILER_CODE AS DSTR_NM,
    NAME AS SUB_CUSTOMER_NAME,
    'NA' AS SUB_CUSTOMER_CODE,
    CAST(customer_code AS VARCHAR) AS CUSTOMER_CODE,
    current_timestamp() as created_dt
FROM SDL_MDS_KR_SUB_CUSTOMER_MASTER
WHERE UPPER(TRIM(RETAILER_CODE)) = 'FOOD_WS'),
final as (
select
cntry_cd::varchar(2) as cntry_cd,
DSTR_NM::varchar(20) as sales_grp,
sub_customer_name::varchar(100) as sub_customer_name,
sub_customer_code::varchar(50) as sub_customer_code,
customer_code::varchar(50) as customer_code,
created_dt::timestamp_ntz(9) as created_dt
from transformed
)
select * from  final
