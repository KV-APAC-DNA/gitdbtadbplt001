with sdl_mds_th_gt_kpi_target as (
select * from {{ source('thasdl_raw', 'sdl_mds_th_gt_kpi_target') }}
),
wks_th_gt_kpi_target_header as (
select * from {{ ref('thawks_integration__wks_th_gt_kpi_target_header') }}
),
transformed as (
select
  'TH' as cntry_cd,
  'THB' as crncy_cd,
  trim(tt.year) as year,
  trim(th.jan) as month,
  trim(tt.kpi_code) as kpi_code,
  trim(tt.kpi_desc) as kpi_desc,
  trim(tt.re_code) as re_code,
  trim(split_part(tt.re_desc, ':', 2)) as re_desc,
  cast(trim(tt.jan) as decimal(18, 6)) as target,
  to_char(cast(current_timestamp() as timestampntz), 'YYYYMMDDHH24MISSFF3') as cdl_dttm,
  current_timestamp() as crtd_dttm,
  current_timestamp() as updt_dttm
from sdl_mds_th_gt_kpi_target as tt, wks_th_gt_kpi_target_header as th
union all
select
  'TH' as cntry_cd,
  'THB' as crncy_cd,
  trim(tt.year) as year,
  trim(th.feb) as month,
  trim(tt.kpi_code) as kpi_code,
  trim(tt.kpi_desc) as kpi_desc,
  trim(tt.re_code) as re_code,
  trim(split_part(tt.re_desc, ':', 2)) as re_desc,
  cast(trim(tt.feb) as decimal(18, 6)) as target,
  to_char(cast(current_timestamp() as timestampntz), 'YYYYMMDDHH24MISSFF3') as cdl_dttm,
  current_timestamp() as crtd_dttm,
  current_timestamp() as updt_dttm
from sdl_mds_th_gt_kpi_target as tt, wks_th_gt_kpi_target_header as th
union all
select
  'TH' as cntry_cd,
  'THB' as crncy_cd,
  trim(tt.year) as year,
  trim(th.mar) as month,
  trim(tt.kpi_code) as kpi_code,
  trim(tt.kpi_desc) as kpi_desc,
  trim(tt.re_code) as re_code,
  trim(split_part(tt.re_desc, ':', 2)) as re_desc,
  cast(trim(tt.mar) as decimal(18, 6)) as target,
  to_char(cast(current_timestamp() as timestampntz), 'YYYYMMDDHH24MISSFF3') as cdl_dttm,
  current_timestamp() as crtd_dttm,
  current_timestamp() as updt_dttm
from sdl_mds_th_gt_kpi_target as tt, wks_th_gt_kpi_target_header as th
union all
select
  'TH' as cntry_cd,
  'THB' as crncy_cd,
  trim(tt.year) as year,
  trim(th.apr) as month,
  trim(tt.kpi_code) as kpi_code,
  trim(tt.kpi_desc) as kpi_desc,
  trim(tt.re_code) as re_code,
  trim(split_part(tt.re_desc, ':', 2)) as re_desc,
  cast(trim(tt.apr) as decimal(18, 6)) as target,
  to_char(cast(current_timestamp() as timestampntz), 'YYYYMMDDHH24MISSFF3') as cdl_dttm,
  current_timestamp() as crtd_dttm,
  current_timestamp() as updt_dttm
from sdl_mds_th_gt_kpi_target as tt, wks_th_gt_kpi_target_header as th
union all
select
  'TH' as cntry_cd,
  'THB' as crncy_cd,
  trim(tt.year) as year,
  trim(th.may) as month,
  trim(tt.kpi_code) as kpi_code,
  trim(tt.kpi_desc) as kpi_desc,
  trim(tt.re_code) as re_code,
  trim(split_part(tt.re_desc, ':', 2)) as re_desc,
  cast(trim(tt.may) as decimal(18, 6)) as target,
  to_char(cast(current_timestamp() as timestampntz), 'YYYYMMDDHH24MISSFF3') as cdl_dttm,
  current_timestamp() as crtd_dttm,
  current_timestamp() as updt_dttm
from sdl_mds_th_gt_kpi_target as tt, wks_th_gt_kpi_target_header as th
union all
select
  'TH' as cntry_cd,
  'THB' as crncy_cd,
  trim(tt.year) as year,
  trim(th.jun) as month,
  trim(tt.kpi_code) as kpi_code,
  trim(tt.kpi_desc) as kpi_desc,
  trim(tt.re_code) as re_code,
  trim(split_part(tt.re_desc, ':', 2)) as re_desc,
  cast(trim(tt.jun) as decimal(18, 6)) as target,
  to_char(cast(current_timestamp() as timestampntz), 'YYYYMMDDHH24MISSFF3') as cdl_dttm,
  current_timestamp() as crtd_dttm,
  current_timestamp() as updt_dttm
from sdl_mds_th_gt_kpi_target as tt, wks_th_gt_kpi_target_header as th
union all
select
  'TH' as cntry_cd,
  'THB' as crncy_cd,
  trim(tt.year) as year,
  trim(th.jul) as month,
  trim(tt.kpi_code) as kpi_code,
  trim(tt.kpi_desc) as kpi_desc,
  trim(tt.re_code) as re_code,
  trim(split_part(tt.re_desc, ':', 2)) as re_desc,
  cast(trim(tt.jul) as decimal(18, 6)) as target,
  to_char(cast(current_timestamp() as timestampntz), 'YYYYMMDDHH24MISSFF3') as cdl_dttm,
  current_timestamp() as crtd_dttm,
  current_timestamp() as updt_dttm
from sdl_mds_th_gt_kpi_target as tt, wks_th_gt_kpi_target_header as th
union all
select
  'TH' as cntry_cd,
  'THB' as crncy_cd,
  trim(tt.year) as year,
  trim(th.aug) as month,
  trim(tt.kpi_code) as kpi_code,
  trim(tt.kpi_desc) as kpi_desc,
  trim(tt.re_code) as re_code,
  trim(split_part(tt.re_desc, ':', 2)) as re_desc,
  cast(trim(tt.aug) as decimal(18, 6)) as target,
  to_char(cast(current_timestamp() as timestampntz), 'YYYYMMDDHH24MISSFF3') as cdl_dttm,
  current_timestamp() as crtd_dttm,
  current_timestamp() as updt_dttm
from sdl_mds_th_gt_kpi_target as tt, wks_th_gt_kpi_target_header as th
union all
select
  'TH' as cntry_cd,
  'THB' as crncy_cd,
  trim(tt.year) as year,
  trim(th.sep) as month,
  trim(tt.kpi_code) as kpi_code,
  trim(tt.kpi_desc) as kpi_desc,
  trim(tt.re_code) as re_code,
  trim(split_part(tt.re_desc, ':', 2)) as re_desc,
  cast(trim(tt.sep) as decimal(18, 6)) as target,
  to_char(cast(current_timestamp() as timestampntz), 'YYYYMMDDHH24MISSFF3') as cdl_dttm,
  current_timestamp() as crtd_dttm,
  current_timestamp() as updt_dttm
from sdl_mds_th_gt_kpi_target as tt, wks_th_gt_kpi_target_header as th
union all
select
  'TH' as cntry_cd,
  'THB' as crncy_cd,
  trim(tt.year) as year,
  trim(th.oct) as month,
  trim(tt.kpi_code) as kpi_code,
  trim(tt.kpi_desc) as kpi_desc,
  trim(tt.re_code) as re_code,
  trim(split_part(tt.re_desc, ':', 2)) as re_desc,
  cast(trim(tt.oct) as decimal(18, 6)) as target,
  to_char(cast(current_timestamp() as timestampntz), 'YYYYMMDDHH24MISSFF3') as cdl_dttm,
  current_timestamp() as crtd_dttm,
  current_timestamp() as updt_dttm
from sdl_mds_th_gt_kpi_target as tt, wks_th_gt_kpi_target_header as th
union all
select
  'TH' as cntry_cd,
  'THB' as crncy_cd,
  trim(tt.year) as year,
  trim(th.nov) as month,
  trim(tt.kpi_code) as kpi_code,
  trim(tt.kpi_desc) as kpi_desc,
  trim(tt.re_code) as re_code,
  trim(split_part(tt.re_desc, ':', 2)) as re_desc,
  cast(trim(tt.nov) as decimal(18, 6)) as target,
  to_char(cast(current_timestamp() as timestampntz), 'YYYYMMDDHH24MISSFF3') as cdl_dttm,
  current_timestamp() as crtd_dttm,
  current_timestamp() as updt_dttm
from sdl_mds_th_gt_kpi_target as tt, wks_th_gt_kpi_target_header as th
union all
select
  'TH' as cntry_cd,
  'THB' as crncy_cd,
  trim(tt.year) as year,
  trim(th.dec) as month,
  trim(tt.kpi_code) as kpi_code,
  trim(tt.kpi_desc) as kpi_desc,
  trim(tt.re_code) as re_code,
  trim(split_part(tt.re_desc, ':', 2)) as re_desc,
  cast(trim(tt.dec) as decimal(18, 6)) as target,
  to_char(cast(current_timestamp() as timestampntz), 'YYYYMMDDHH24MISSFF3') as cdl_dttm,
  current_timestamp() as crtd_dttm,
  current_timestamp() as updt_dttm
from sdl_mds_th_gt_kpi_target as tt, wks_th_gt_kpi_target_header as th
     ),
final as (
    select
    cntry_cd::varchar(5) as cntry_cd,
    crncy_cd::varchar(5) as crncy_cd,
    year::varchar(5) as year,
    month::varchar(5) as month,
    kpi_code::varchar(100) as kpi_code,
    kpi_desc::varchar(100) as kpi_desc,
    re_code::varchar(50) as re_code,
    re_desc::varchar(100) as re_desc,
    target::number(18,6) as target,
    cdl_dttm::varchar(100) as cdl_dttm,
    crtd_dttm::timestamp_ntz(9) as crtd_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
    from transformed
)
select * from final

