with sdl_mds_ph_targets_by_account_and_skus as (
    select * from {{ source('phlsdl_raw', 'sdl_mds_ph_targets_by_account_and_skus') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
transformed as (
SELECT year,

       month,

       account_code as account,

       activity,

       area,

       channel,

       sku_code,

       brand,

       target_value,

       current_timestamp() AS crt_dttm,

       NULL AS updt_dttm

FROM (SELECT CAST(YEAR AS VARCHAR) AS YEAR,

             DECODE(mnth_long,

                   'JANUARY','01',

                   'FEBRUARY','02',

                   'MARCH','03',

                   'APRIL','04',

                   'MAY','05',

                   'JUNE','06',

                   'JULY','07',

                   'AUGUST','08',

                   'SEPTEMBER','09',

                   'OCTOBER','10',

                   'NOVEMBER','11',

                   'DECEMBER','12'

             ) AS MONTH,

             account_code,

             activity,

             area,

             channel,

             CAST(sku_code AS VARCHAR) AS sku_code,

             brand,

             DECODE(mnth_long,

                   'JANUARY',mar,

                   'FEBRUARY',mar,

                   'MARCH',mar,

                   'APRIL',apr,

                   'MAY',MAY,

                   'JUNE',jun,

                   'JULY',jul,

                   'AUGUST',aug,

                   'SEPTEMBER',sep,

                   'OCTOBER',oct,

                   'NOVEMBER',nov,

                   'DECEMBER',dec

             ) AS target_value

      FROM sdl_mds_ph_targets_by_account_and_skus AS t1,

           (SELECT DISTINCT mnth_long FROM edw_vw_os_time_dim) AS t2)
),
final as (
select
year::varchar(10) as year,
month::varchar(10) as month,
account::varchar(255) as account_nm,
activity::varchar(255) as activity,
area::varchar(255) as area,
channel::varchar(255) as channel,
sku_code::varchar(50) as item_cd,
brand::varchar(255) as brand,
target_value::number(31,7) as target_value,
crt_dttm::timestamp_ntz(9) as crtd_dttm,
updt_dttm::timestamp_ntz(9) as updt_dttm
from transformed
)
select * from final

       


       

       




      