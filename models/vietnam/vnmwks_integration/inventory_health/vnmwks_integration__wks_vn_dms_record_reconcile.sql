{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with sdl_vn_dms_data_extract_summary as(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_data_extract_summary') }} 
),
sdl_vn_dms_call_details as (
       select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_call_details') }} 
       where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_call_details__duplicate_test')}}
    )
),
itg_vn_dms_call_details as (
       select * from {{ ref('vnmitg_integration__itg_vn_dms_call_details') }} 
),
sdl_vn_dms_customer_dim as (
       select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_customer_dim') }} 
       where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_customer_dim__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_customer_dim__duplicate_test')}}
    )
),
itg_vn_dms_customer_dim as (
       select * from {{ ref('vnmitg_integration__itg_vn_dms_customer_dim') }}
),
sdl_vn_dms_d_sellout_sales_fact as (
       select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_d_sellout_sales_fact') }} 
       where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_d_sellout_sales_fact__duplicate_test')}}
    )
),
itg_vn_dms_d_sellout_sales_fact as (
       select * from {{ ref('vnmitg_integration__itg_vn_dms_d_sellout_sales_fact') }}
),
sdl_vn_dms_distributor_dim as (
       select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_distributor_dim') }} 
       where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_distributor_dim__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_distributor_dim__duplicate_test')}}
    )
),
itg_vn_dms_distributor_dim as (
       select * from {{ ref('vnmitg_integration__itg_vn_dms_distributor_dim') }}

),
sdl_vn_dms_forecast as (
       select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_forecast') }} 
       where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_forecast__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_forecast__duplicate_test')}}
       )
),
itg_vn_dms_forecast as (
       select * from {{ ref('vnmitg_integration__itg_vn_dms_forecast') }}
),
sdl_vn_dms_h_sellout_sales_fact as (
       select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_h_sellout_sales_fact') }} 
       where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_h_sellout_sales_fact__duplicate_test')}}
       )
),
itg_vn_dms_h_sellout_sales_fact as (
       select * from {{ ref('vnmitg_integration__itg_vn_dms_h_sellout_sales_fact') }}
),
sdl_vn_dms_history_saleout as (
       select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_history_saleout') }} 
       where SOURCE_FILE_NAME not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_history_saleout__duplicate_test')}}
    )
),
itg_vn_dms_history_saleout as (
       select * from {{ ref('vnmitg_integration__itg_vn_dms_history_saleout') }}
),
sdl_vn_dms_kpi as (
       select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_kpi') }}
       where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_kpi__duplicate_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_kpi__null_test')}}
    ) 
),
itg_vn_dms_kpi as (
       select * from {{ ref('vnmitg_integration__itg_vn_dms_kpi') }}
),
sdl_vn_dms_kpi_sellin_sellthrgh as (
       select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_kpi_sellin_sellthrgh') }} 
       where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_kpi_sellin_sellthrgh__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_kpi_sellin_sellthrgh__duplicate_test')}}
        )
),
itg_vn_dms_kpi_sellin_sellthrgh as (
       select * from {{ ref('vnmitg_integration__itg_vn_dms_kpi_sellin_sellthrgh') }}
),
sdl_vn_dms_msl as (
       select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_msl') }} 
       where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_msl__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_msl__duplicate_test')}}
    )
),
itg_vn_dms_msl as (
       select * from {{ ref('vnmitg_integration__itg_vn_dms_msl') }}
),
sdl_vn_dms_order_promotion as (
       select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_order_promotion') }} 
       where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_order_promotion__duplicate_test')}}
    )
),
itg_vn_dms_order_promotion as (
       select * from {{ ref('vnmitg_integration__itg_vn_dms_order_promotion') }}
),
sdl_vn_dms_product_dim as (
       select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_product_dim') }} 
       where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_product_dim__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_product_dim__duplicate_test')}}
     ) 
),
itg_vn_dms_product_dim as (
       select * from {{ ref('vnmitg_integration__itg_vn_dms_product_dim') }}
),
sdl_vn_dms_promotion_list as (
       select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_promotion_list') }} 
       where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_promotion_list__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_promotion_list__duplicate_test')}}
     ) 
),
itg_vn_dms_promotion_list as (
       select * from {{ ref('vnmitg_integration__itg_vn_dms_promotion_list') }}
),
sdl_vn_dms_sales_org_dim as (
       select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_sales_org_dim') }} 
       where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_sales_org_dim__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_sales_org_dim__duplicate_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_sales_org_dim__test_format')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_sales_org_dim__test_format2')}}
    )
),
itg_vn_dms_sales_org_dim as (
       select * from {{ ref('vnmitg_integration__itg_vn_dms_sales_org_dim') }}
),
sdl_vn_dms_sales_stock_fact as (
       select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_sales_stock_fact') }} 
       where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_sales_stock_fact__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_sales_stock_fact__duplicate_test')}}
    ) 
),
itg_vn_dms_sales_stock_fact as (
       select * from {{ ref('vnmitg_integration__itg_vn_dms_sales_stock_fact') }}
),
sdl_vn_dms_sellthrgh_sales_fact as (
       select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_sellthrgh_sales_fact') }} 
       where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_sellthrgh_sales_fact__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_sellthrgh_sales_fact__duplicate_test')}}
        )
),
itg_vn_dms_sellthrgh_sales_fact as (
       select * from {{ ref('vnmitg_integration__itg_vn_dms_sellthrgh_sales_fact') }}
),
sdl_vn_dms_yearly_target as (
       select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_yearly_target') }} 
       where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_yearly_target__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_yearly_target__duplicate_test')}}
    ) 
),
sdl_raw_vn_dms_yearly_target as (
       select * from {{ ref('vnmitg_integration__sdl_raw_vn_dms_yearly_target') }} 
),
itg_vn_dms_yearly_target as (
       select * from {{ ref('vnmitg_integration__itg_vn_dms_yearly_target') }}
),

union1 as( 
SELECT DISTINCT
  TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9))) AS recon_date,
  SDL.file_name,
  SRC.sourcefile_count,
  SDL.sdl_count,
  SDL_RAW.sdl_raw_count,
  ITG.itg_count,
  (
    SRC.sourcefile_count - SDL.sdl_count
  ) AS "diff_SRC_bw_SDL",
  (
    SDL.sdl_count - SDL_RAW.sdl_raw_count
  ) AS "diff_SDL_bw_SDL_RAW",
  (
    SDL.sdl_count - ITG.itg_count
  ) AS "diff_SDL_bw_ITG"
FROM (
  SELECT
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 1)
    ) AS source_file_name,
    record_count AS sourcefile_count
  FROM sdl_vn_dms_data_extract_summary
  WHERE
    --to_date(date_of_extraction) = TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9)))
    to_date(date_of_extraction::varchar) = TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9)))
) AS SRC, (
  SELECT
    source_file_name AS file_name,
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 5)
    ) AS source_file_name,
    run_id,
    COUNT(*) AS sdl_count
  FROM sdl_vn_dms_call_details
  GROUP BY
    source_file_name,
    run_id
) AS SDL, (
  SELECT
    run_id,
    COUNT(*) AS sdl_raw_count
  FROM itg_vn_dms_call_details
  GROUP BY
    run_id
) AS SDL_RAW, (
  SELECT
    run_id,
    COUNT(*) AS itg_count
  FROM itg_vn_dms_call_details
  GROUP BY
    run_id
) AS ITG
WHERE
  SRC.source_file_name = SDL.source_file_name
  AND SDL.run_id = SDL_RAW.run_id
  AND SDL_RAW.run_id = ITG.run_id /* CALL DETAILS */
),
union2 as(
SELECT DISTINCT
  TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9))) AS recon_date,
  SDL.file_name,
  SRC.sourcefile_count,
  SDL.sdl_count,
  SDL_RAW.sdl_raw_count,
  ITG.itg_count,
  (
    SRC.sourcefile_count - SDL.sdl_count
  ) AS "diff_SRC_bw_SDL",
  (
    SDL.sdl_count - SDL_RAW.sdl_raw_count
  ) AS "diff_SDL_bw_SDL_RAW",
  (
    SDL.sdl_count - ITG.itg_count
  ) AS "diff_SDL_bw_ITG"
FROM (
  SELECT
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 1)
    ) AS source_file_name,
    record_count AS sourcefile_count
  FROM sdl_vn_dms_data_extract_summary
  WHERE
    to_date(date_of_extraction) = TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9)))
) AS SRC, (
  SELECT
    source_file_name AS file_name,
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 5)
    ) AS source_file_name,
    run_id,
    COUNT(*) AS sdl_count
  FROM sdl_vn_dms_customer_dim
  GROUP BY
    source_file_name,
    run_id
) AS SDL, (
  SELECT
    run_id,
    COUNT(*) AS sdl_raw_count
  FROM itg_vn_dms_customer_dim
  GROUP BY
    run_id
) AS SDL_RAW, (
  SELECT
    run_id,
    COUNT(*) AS itg_count
  FROM itg_vn_dms_customer_dim
  GROUP BY
    run_id
) AS ITG
WHERE
  SRC.source_file_name = SDL.source_file_name
  AND SDL.run_id = SDL_RAW.run_id
  AND SDL_RAW.run_id = ITG.run_id /* CUSTOMER */
),
union3 as(
SELECT DISTINCT
  TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9))) AS recon_date,
  SDL.file_name,
  SRC.sourcefile_count,
  SDL.sdl_count,
  SDL_RAW.sdl_raw_count,
  ITG.itg_count,
  (
    SRC.sourcefile_count - SDL.sdl_count
  ) AS "diff_SRC_bw_SDL",
  (
    SDL.sdl_count - SDL_RAW.sdl_raw_count
  ) AS "diff_SDL_bw_SDL_RAW",
  (
    SDL.sdl_count - ITG.itg_count
  ) AS "diff_SDL_bw_ITG"
FROM (
  SELECT
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 1)
    ) AS source_file_name,
    record_count AS sourcefile_count
  FROM sdl_vn_dms_data_extract_summary
  WHERE
    to_date(date_of_extraction) = TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9)))
) AS SRC, (
  SELECT
    source_file_name AS file_name,
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 5)
    ) AS source_file_name,
    run_id,
    COUNT(*) AS sdl_count
  FROM sdl_vn_dms_d_sellout_sales_fact
  GROUP BY
    source_file_name,
    run_id
) AS SDL, (
  SELECT
    run_id,
    COUNT(*) AS sdl_raw_count
  FROM itg_vn_dms_d_sellout_sales_fact
  GROUP BY
    run_id
) AS SDL_RAW, (
  SELECT
    run_id,
    COUNT(*) AS itg_count
  FROM itg_vn_dms_d_sellout_sales_fact
  GROUP BY
    run_id
) AS ITG
WHERE
  SRC.source_file_name = SDL.source_file_name
  AND SDL.run_id = SDL_RAW.run_id
  AND SDL_RAW.run_id = ITG.run_id /* SELLOUT DETAILS */
),
union4 as(
SELECT DISTINCT
  TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9))) AS recon_date,
  SDL.file_name,
  SRC.sourcefile_count,
  SDL.sdl_count,
  SDL_RAW.sdl_raw_count,
  ITG.itg_count,
  (
    SRC.sourcefile_count - SDL.sdl_count
  ) AS "diff_SRC_bw_SDL",
  (
    SDL.sdl_count - SDL_RAW.sdl_raw_count
  ) AS "diff_SDL_bw_SDL_RAW",
  (
    SDL.sdl_count - ITG.itg_count
  ) AS "diff_SDL_bw_ITG"
FROM (
  SELECT
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 1)
    ) AS source_file_name,
    record_count AS sourcefile_count
  FROM sdl_vn_dms_data_extract_summary
  WHERE
    to_date(date_of_extraction) = TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9)))
) AS SRC, (
  SELECT
    source_file_name AS file_name,
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 5)
    ) AS source_file_name,
    run_id,
    COUNT(*) AS sdl_count
  FROM sdl_vn_dms_distributor_dim
  GROUP BY
    source_file_name,
    run_id
) AS SDL, (
  SELECT
    run_id,
    COUNT(*) AS sdl_raw_count
  FROM itg_vn_dms_distributor_dim
  GROUP BY
    run_id
) AS SDL_RAW, (
  SELECT
    run_id,
    COUNT(*) AS itg_count
  FROM itg_vn_dms_distributor_dim
  GROUP BY
    run_id
) AS ITG
WHERE
  SRC.source_file_name = SDL.source_file_name
  AND SDL.run_id = SDL_RAW.run_id
  AND SDL_RAW.run_id = ITG.run_id /* Distributor_dim */
),
union5 as(
SELECT DISTINCT
  TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9))) AS recon_date,
  SDL.file_name,
  SRC.sourcefile_count,
  SDL.sdl_count,
  SDL_RAW.sdl_raw_count,
  ITG.itg_count,
  (
    SRC.sourcefile_count - SDL.sdl_count
  ) AS "diff_SRC_bw_SDL",
  (
    SDL.sdl_count - SDL_RAW.sdl_raw_count
  ) AS "diff_SDL_bw_SDL_RAW",
  (
    SDL.sdl_count - ITG.itg_count
  ) AS "diff_SDL_bw_ITG"
FROM (
  SELECT
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 1)
    ) AS source_file_name,
    record_count AS sourcefile_count
  FROM sdl_vn_dms_data_extract_summary
  WHERE
    to_date(date_of_extraction) = TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9)))
) AS SRC, (
  SELECT
    source_file_name AS file_name,
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 5)
    ) AS source_file_name,
    run_id,
    COUNT(*) AS sdl_count
  FROM sdl_vn_dms_forecast
  GROUP BY
    source_file_name,
    run_id
) AS SDL, (
  SELECT
    run_id,
    COUNT(*) AS sdl_raw_count
  FROM itg_vn_dms_forecast
  GROUP BY
    run_id
) AS SDL_RAW, (
  SELECT
    run_id,
    COUNT(*) AS itg_count
  FROM itg_vn_dms_forecast
  GROUP BY
    run_id
) AS ITG
WHERE
  SRC.source_file_name = SDL.source_file_name
  AND SDL.run_id = SDL_RAW.run_id
  AND SDL_RAW.run_id = ITG.run_id /* Forecast */
),
union6 as(
SELECT DISTINCT
  TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9))) AS recon_date,
  SDL.file_name,
  SRC.sourcefile_count,
  SDL.sdl_count,
  SDL_RAW.sdl_raw_count,
  ITG.itg_count,
  (
    SRC.sourcefile_count - SDL.sdl_count
  ) AS "diff_SRC_bw_SDL",
  (
    SDL.sdl_count - SDL_RAW.sdl_raw_count
  ) AS "diff_SDL_bw_SDL_RAW",
  (
    SDL.sdl_count - ITG.itg_count
  ) AS "diff_SDL_bw_ITG"
FROM (
  SELECT
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 1)
    ) AS source_file_name,
    record_count AS sourcefile_count
  FROM sdl_vn_dms_data_extract_summary
  WHERE
    to_date(date_of_extraction) = TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9)))
) AS SRC, (
  SELECT
    source_file_name AS file_name,
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 5)
    ) AS source_file_name,
    run_id,
    COUNT(*) AS sdl_count
  FROM sdl_vn_dms_h_sellout_sales_fact
  GROUP BY
    source_file_name,
    run_id
) AS SDL, (
  SELECT
    run_id,
    COUNT(*) AS sdl_raw_count
  FROM itg_vn_dms_h_sellout_sales_fact
  GROUP BY
    run_id
) AS SDL_RAW, (
  SELECT
    run_id,
    COUNT(*) AS itg_count
  FROM itg_vn_dms_h_sellout_sales_fact
  GROUP BY
    run_id
) AS ITG
WHERE
  SRC.source_file_name = SDL.source_file_name
  AND SDL.run_id = SDL_RAW.run_id
  AND SDL_RAW.run_id = ITG.run_id /* SELLOUT Header */
),
union7 as(
SELECT DISTINCT
  TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9))) AS recon_date,
  SDL.file_name,
  SRC.sourcefile_count,
  SDL.sdl_count,
  SDL_RAW.sdl_raw_count,
  ITG.itg_count,
  (
    SRC.sourcefile_count - SDL.sdl_count
  ) AS "diff_SRC_bw_SDL",
  (
    SDL.sdl_count - SDL_RAW.sdl_raw_count
  ) AS "diff_SDL_bw_SDL_RAW",
  (
    SDL.sdl_count - ITG.itg_count
  ) AS "diff_SDL_bw_ITG"
FROM (
  SELECT
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 1)
    ) AS source_file_name,
    record_count AS sourcefile_count
  FROM sdl_vn_dms_data_extract_summary
  WHERE
    to_date(date_of_extraction) = TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9)))
) AS SRC, (
  SELECT
    source_file_name AS file_name,
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 5)
    ) AS source_file_name,
    run_id,
    COUNT(*) AS sdl_count
  FROM sdl_vn_dms_history_saleout
  GROUP BY
    source_file_name,
    run_id
) AS SDL, (
  SELECT
    run_id,
    COUNT(*) AS sdl_raw_count
  FROM itg_vn_dms_history_saleout
  GROUP BY
    run_id
) AS SDL_RAW, (
  SELECT
    run_id,
    COUNT(*) AS itg_count
  FROM itg_vn_dms_history_saleout
  GROUP BY
    run_id
) AS ITG
WHERE
  SRC.source_file_name = SDL.source_file_name
  AND SDL.run_id = SDL_RAW.run_id
  AND SDL_RAW.run_id = ITG.run_id /* History saleout */
),
union8 as(
SELECT DISTINCT
  TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9))) AS recon_date,
  SDL.file_name,
  SRC.sourcefile_count,
  SDL.sdl_count,
  SDL_RAW.sdl_raw_count,
  ITG.itg_count,
  (
    SRC.sourcefile_count - SDL.sdl_count
  ) AS "diff_SRC_bw_SDL",
  (
    SDL.sdl_count - SDL_RAW.sdl_raw_count
  ) AS "diff_SDL_bw_SDL_RAW",
  (
    SDL.sdl_count - ITG.itg_count
  ) AS "diff_SDL_bw_ITG"
FROM (
  SELECT
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 1)
    ) AS source_file_name,
    record_count AS sourcefile_count
  FROM sdl_vn_dms_data_extract_summary
  WHERE
    to_date(date_of_extraction) = TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9)))
) AS SRC, (
  SELECT
    source_file_name AS file_name,
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 5)
    ) AS source_file_name,
    run_id,
    COUNT(*) AS sdl_count
  FROM sdl_vn_dms_kpi
  GROUP BY
    source_file_name,
    run_id
) AS SDL, (
  SELECT
    run_id,
    COUNT(*) AS sdl_raw_count
  FROM itg_vn_dms_kpi
  GROUP BY
    run_id
) AS SDL_RAW, (
  SELECT
    run_id,
    COUNT(*) AS itg_count
  FROM itg_vn_dms_kpi
  GROUP BY
    run_id
) AS ITG
WHERE
  SRC.source_file_name = SDL.source_file_name
  AND SDL.run_id = SDL_RAW.run_id
  AND SDL_RAW.run_id = ITG.run_id /* KPI */
),
union9 as(
SELECT DISTINCT
  TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9))) AS recon_date,
  SDL.file_name,
  SRC.sourcefile_count,
  SDL.sdl_count,
  SDL_RAW.sdl_raw_count,
  ITG.itg_count,
  (
    SRC.sourcefile_count - SDL.sdl_count
  ) AS "diff_SRC_bw_SDL",
  (
    SDL.sdl_count - SDL_RAW.sdl_raw_count
  ) AS "diff_SDL_bw_SDL_RAW",
  (
    SDL.sdl_count - ITG.itg_count
  ) AS "diff_SDL_bw_ITG"
FROM (
  SELECT
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 1)
    ) AS source_file_name,
    record_count AS sourcefile_count
  FROM sdl_vn_dms_data_extract_summary
  WHERE
    to_date(date_of_extraction) = TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9)))
) AS SRC, (
  SELECT
    source_file_name AS file_name,
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 5)
    ) AS source_file_name,
    run_id,
    COUNT(*) AS sdl_count
  FROM sdl_vn_dms_kpi_sellin_sellthrgh
  GROUP BY
    source_file_name,
    run_id
) AS SDL, (
  SELECT
    run_id,
    COUNT(*) AS sdl_raw_count
  FROM itg_vn_dms_kpi_sellin_sellthrgh
  GROUP BY
    run_id
) AS SDL_RAW, (
  SELECT
    run_id,
    COUNT(*) AS itg_count
  FROM itg_vn_dms_kpi_sellin_sellthrgh
  GROUP BY
    run_id
) AS ITG
WHERE
  SRC.source_file_name = SDL.source_file_name
  AND SDL.run_id = SDL_RAW.run_id
  AND SDL_RAW.run_id = ITG.run_id /* KPI Sellin Sellthrough */
),
union10 as(
SELECT DISTINCT
  TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9))) AS recon_date,
  SDL.file_name,
  SRC.sourcefile_count,
  SDL.sdl_count,
  SDL_RAW.sdl_raw_count,
  ITG.itg_count,
  (
    SRC.sourcefile_count - SDL.sdl_count
  ) AS "diff_SRC_bw_SDL",
  (
    SDL.sdl_count - SDL_RAW.sdl_raw_count
  ) AS "diff_SDL_bw_SDL_RAW",
  (
    SDL.sdl_count - ITG.itg_count
  ) AS "diff_SDL_bw_ITG"
FROM (
  SELECT
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 1)
    ) AS source_file_name,
    record_count AS sourcefile_count
  FROM sdl_vn_dms_data_extract_summary
  WHERE
    to_date(date_of_extraction) = TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9)))
) AS SRC, (
  SELECT
    source_file_name AS file_name,
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 5)
    ) AS source_file_name,
    run_id,
    COUNT(*) AS sdl_count
  FROM sdl_vn_dms_msl
  GROUP BY
    source_file_name,
    run_id
) AS SDL, (
  SELECT
    run_id,
    COUNT(*) AS sdl_raw_count
  FROM itg_vn_dms_msl
  GROUP BY
    run_id
) AS SDL_RAW, (
  SELECT
    run_id,
    COUNT(*) AS itg_count
  FROM itg_vn_dms_msl
  GROUP BY
    run_id
) AS ITG
WHERE
  SRC.source_file_name = SDL.source_file_name
  AND SDL.run_id = SDL_RAW.run_id
  AND SDL_RAW.run_id = ITG.run_id /* MSL */
),
union11 as(
SELECT DISTINCT
  TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9))) AS recon_date,
  SDL.file_name,
  SRC.sourcefile_count,
  SDL.sdl_count,
  SDL_RAW.sdl_raw_count,
  ITG.itg_count,
  (
    SRC.sourcefile_count - SDL.sdl_count
  ) AS "diff_SRC_bw_SDL",
  (
    SDL.sdl_count - SDL_RAW.sdl_raw_count
  ) AS "diff_SDL_bw_SDL_RAW",
  (
    SDL.sdl_count - ITG.itg_count
  ) AS "diff_SDL_bw_ITG"
FROM (
  SELECT
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 1)
    ) AS source_file_name,
    record_count AS sourcefile_count
  FROM sdl_vn_dms_data_extract_summary
  WHERE
    to_date(date_of_extraction) = TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9)))
) AS SRC, (
  SELECT
    source_file_name AS file_name,
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 5)
    ) AS source_file_name,
    run_id,
    COUNT(*) AS sdl_count
  FROM sdl_vn_dms_order_promotion
  GROUP BY
    source_file_name,
    run_id
) AS SDL, (
  SELECT
    run_id,
    COUNT(*) AS sdl_raw_count
  FROM itg_vn_dms_order_promotion
  GROUP BY
    run_id
) AS SDL_RAW, (
  SELECT
    run_id,
    COUNT(*) AS itg_count
  FROM itg_vn_dms_order_promotion
  GROUP BY
    run_id
) AS ITG
WHERE
  SRC.source_file_name = SDL.source_file_name
  AND SDL.run_id = SDL_RAW.run_id
  AND SDL_RAW.run_id = ITG.run_id /* Order Promotion */
),
union12 as(
SELECT DISTINCT
  TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9))) AS recon_date,
  SDL.file_name,
  SRC.sourcefile_count,
  SDL.sdl_count,
  SDL_RAW.sdl_raw_count,
  ITG.itg_count,
  (
    SRC.sourcefile_count - SDL.sdl_count
  ) AS "diff_SRC_bw_SDL",
  (
    SDL.sdl_count - SDL_RAW.sdl_raw_count
  ) AS "diff_SDL_bw_SDL_RAW",
  (
    SDL.sdl_count - ITG.itg_count
  ) AS "diff_SDL_bw_ITG"
FROM (
  SELECT
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 1)
    ) AS source_file_name,
    record_count AS sourcefile_count
  FROM sdl_vn_dms_data_extract_summary
  WHERE
    to_date(date_of_extraction) = TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9)))
) AS SRC, (
  SELECT
    source_file_name AS file_name,
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 5)
    ) AS source_file_name,
    run_id,
    COUNT(*) AS sdl_count
  FROM sdl_vn_dms_product_dim
  GROUP BY
    source_file_name,
    run_id
) AS SDL, (
  SELECT
    run_id,
    COUNT(*) AS sdl_raw_count
  FROM itg_vn_dms_product_dim
  GROUP BY
    run_id
) AS SDL_RAW, (
  SELECT
    run_id,
    COUNT(*) AS itg_count
  FROM itg_vn_dms_product_dim
  GROUP BY
    run_id
) AS ITG
WHERE
  SRC.source_file_name = SDL.source_file_name
  AND SDL.run_id = SDL_RAW.run_id
  AND SDL_RAW.run_id = ITG.run_id /* Product_dim */
),
union13 as(
SELECT DISTINCT
  TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9))) AS recon_date,
  SDL.file_name,
  SRC.sourcefile_count,
  SDL.sdl_count,
  SDL_RAW.sdl_raw_count,
  ITG.itg_count,
  (
    SRC.sourcefile_count - SDL.sdl_count
  ) AS "diff_SRC_bw_SDL",
  (
    SDL.sdl_count - SDL_RAW.sdl_raw_count
  ) AS "diff_SDL_bw_SDL_RAW",
  (
    SDL.sdl_count - ITG.itg_count
  ) AS "diff_SDL_bw_ITG"
FROM (
  SELECT
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 1)
    ) AS source_file_name,
    record_count AS sourcefile_count
  FROM sdl_vn_dms_data_extract_summary
  WHERE
    to_date(date_of_extraction) = TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9)))
) AS SRC, (
  SELECT
    source_file_name AS file_name,
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 5)
    ) AS source_file_name,
    run_id,
    COUNT(*) AS sdl_count
  FROM sdl_vn_dms_promotion_list
  GROUP BY
    source_file_name,
    run_id
) AS SDL, (
  SELECT
    run_id,
    COUNT(*) AS sdl_raw_count
  FROM itg_vn_dms_promotion_list
  GROUP BY
    run_id
) AS SDL_RAW, (
  SELECT
    run_id,
    COUNT(*) AS itg_count
  FROM itg_vn_dms_promotion_list
  GROUP BY
    run_id
) AS ITG
WHERE
  SRC.source_file_name = SDL.source_file_name
  AND SDL.run_id = SDL_RAW.run_id
  AND SDL_RAW.run_id = ITG.run_id /* Promotion List */
),
union14 as(
SELECT DISTINCT
  TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9))) AS recon_date,
  SDL.file_name,
  SRC.sourcefile_count,
  SDL.sdl_count,
  SDL_RAW.sdl_raw_count,
  ITG.itg_count,
  (
    SRC.sourcefile_count - SDL.sdl_count
  ) AS "diff_SRC_bw_SDL",
  (
    SDL.sdl_count - SDL_RAW.sdl_raw_count
  ) AS "diff_SDL_bw_SDL_RAW",
  (
    SDL.sdl_count - ITG.itg_count
  ) AS "diff_SDL_bw_ITG"
FROM (
  SELECT
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 1)
    ) AS source_file_name,
    record_count AS sourcefile_count
  FROM sdl_vn_dms_data_extract_summary
  WHERE
    to_date(date_of_extraction) = TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9)))
) AS SRC, (
  SELECT
    source_file_name AS file_name,
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 5)
    ) AS source_file_name,
    run_id,
    COUNT(*) AS sdl_count
  FROM sdl_vn_dms_sales_org_dim
  GROUP BY
    source_file_name,
    run_id
) AS SDL, (
  SELECT
    run_id,
    COUNT(*) AS sdl_raw_count
  FROM itg_vn_dms_sales_org_dim
  GROUP BY
    run_id
) AS SDL_RAW, (
  SELECT
    run_id,
    COUNT(*) AS itg_count
  FROM itg_vn_dms_sales_org_dim
  GROUP BY
    run_id
) AS ITG
WHERE
  SRC.source_file_name = SDL.source_file_name
  AND SDL.run_id = SDL_RAW.run_id
  AND SDL_RAW.run_id = ITG.run_id /* Sales org */
),
union15 as(
SELECT DISTINCT
  TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9))) AS recon_date,
  SDL.file_name,
  SRC.sourcefile_count,
  SDL.sdl_count,
  SDL_RAW.sdl_raw_count,
  ITG.itg_count,
  (
    SRC.sourcefile_count - SDL.sdl_count
  ) AS "diff_SRC_bw_SDL",
  (
    SDL.sdl_count - SDL_RAW.sdl_raw_count
  ) AS "diff_SDL_bw_SDL_RAW",
  (
    SDL.sdl_count - ITG.itg_count
  ) AS "diff_SDL_bw_ITG"
FROM (
  SELECT
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 1)
    ) AS source_file_name,
    record_count AS sourcefile_count
  FROM sdl_vn_dms_data_extract_summary
  WHERE
    to_date(date_of_extraction) = TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9)))
) AS SRC, (
  SELECT
    source_file_name AS file_name,
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 5)
    ) AS source_file_name,
    run_id,
    COUNT(*) AS sdl_count
  FROM sdl_vn_dms_sales_stock_fact
  GROUP BY
    source_file_name,
    run_id
) AS SDL, (
  SELECT
    run_id,
    COUNT(*) AS sdl_raw_count
  FROM itg_vn_dms_sales_stock_fact
  GROUP BY
    run_id
) AS SDL_RAW, (
  SELECT
    run_id,
    COUNT(*) AS itg_count
  FROM itg_vn_dms_sales_stock_fact
  GROUP BY
    run_id
) AS ITG
WHERE
  SRC.source_file_name = SDL.source_file_name
  AND SDL.run_id = SDL_RAW.run_id
  AND SDL_RAW.run_id = ITG.run_id /* sales stock */
),
union16 as(
SELECT DISTINCT
  TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9))) AS recon_date,
  SDL.file_name,
  SRC.sourcefile_count,
  SDL.sdl_count,
  SDL_RAW.sdl_raw_count,
  ITG.itg_count,
  (
    SRC.sourcefile_count - SDL.sdl_count
  ) AS "diff_SRC_bw_SDL",
  (
    SDL.sdl_count - SDL_RAW.sdl_raw_count
  ) AS "diff_SDL_bw_SDL_RAW",
  (
    SDL.sdl_count - ITG.itg_count
  ) AS "diff_SDL_bw_ITG"
FROM (
  SELECT
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 1)
    ) AS source_file_name,
    record_count AS sourcefile_count
  FROM sdl_vn_dms_data_extract_summary
  WHERE
    to_date(date_of_extraction) = TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9)))
) AS SRC, (
  SELECT
    source_file_name AS file_name,
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 5)
    ) AS source_file_name,
    run_id,
    COUNT(*) AS sdl_count
  FROM sdl_vn_dms_sellthrgh_sales_fact
  GROUP BY
    source_file_name,
    run_id
) AS SDL, (
  SELECT
    run_id,
    COUNT(*) AS sdl_raw_count
  FROM itg_vn_dms_sellthrgh_sales_fact
  GROUP BY
    run_id
) AS SDL_RAW, (
  SELECT
    run_id,
    COUNT(*) AS itg_count
  FROM itg_vn_dms_sellthrgh_sales_fact
  GROUP BY
    run_id
) AS ITG
WHERE
  SRC.source_file_name = SDL.source_file_name
  AND SDL.run_id = SDL_RAW.run_id /* Sell Through */
  AND SDL_RAW.run_id = ITG.run_id
),
union17 as(
SELECT DISTINCT
  TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9))) AS recon_date,
  SDL.file_name,
  SRC.sourcefile_count,
  SDL.sdl_count,
  SDL_RAW.sdl_raw_count,
  ITG.itg_count,
  (
    SRC.sourcefile_count - SDL.sdl_count
  ) AS "diff_SRC_bw_SDL",
  (
    SDL.sdl_count - SDL_RAW.sdl_raw_count
  ) AS "diff_SDL_bw_SDL_RAW",
  (
    SDL.sdl_count - ITG.itg_count
  ) AS "diff_SDL_bw_ITG"
FROM (
  SELECT
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 1)
    ) AS source_file_name,
    record_count AS sourcefile_count
  FROM sdl_vn_dms_data_extract_summary
  WHERE
    to_date(date_of_extraction) = TO_DATE(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()::timestamp_ntz(9)))
) AS SRC, (
  SELECT
    source_file_name AS file_name,
    (
      SUBSTRING(source_file_name, 1, REGEXP_INSTR(source_file_name, '.csv') - 1)
    ) AS source_file_name,
    run_id,
    COUNT(*) AS sdl_count
  FROM sdl_vn_dms_yearly_target
  GROUP BY
    source_file_name,
    run_id
) AS SDL, (
  SELECT
    run_id,
    COUNT(*) AS sdl_raw_count
  FROM sdl_raw_vn_dms_yearly_target
  GROUP BY
    run_id
) AS SDL_RAW, (
  SELECT
    run_id,
    COUNT(*) AS itg_count
  FROM itg_vn_dms_yearly_target
  GROUP BY
    run_id
) AS ITG
WHERE
  SRC.source_file_name = SDL.source_file_name
  AND SDL.run_id = SDL_RAW.run_id /* Yearly Target */
  AND SDL_RAW.run_id = ITG.run_id
),
transformed as(
    select * from union1
    union
    select * from union2
    union
    select * from union3
    union
    select * from union4
    union
    select * from union5
    union
    select * from union6
    union
    select * from union7
    union
    select * from union8
    union
    select * from union9
    union
    select * from union10
    union
    select * from union11
    union
    select * from union12
    union
    select * from union13
    union
    select * from union14
    union
    select * from union15
    union
    select * from union16
    union
    select * from union17
),
final as(
    select * from transformed

    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where transformed.recon_date > (select max(recon_date) from {{ this }}) 
    {% endif %}
)
select * from final