{{
    config
    (
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="{% if is_incremental() %}
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_cd)),UPPER(TRIM(article_cd)),UPPER(TRIM(level))) 
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_code)),UPPER(TRIM(article_code)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_hg') }} WHERE UPPER(level) = 'J&J') AND UPPER(level) = 'J&J';
                        
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_hg') }} WHERE UPPER(level) = 'CATEGORY') AND UPPER(level) = 'CATEGORY';
                    

                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_cd)),UPPER(TRIM(article_cd)),UPPER(TRIM(level)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_code)),UPPER(TRIM(article_code)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_max') }} WHERE UPPER(level) = 'J&J') AND UPPER(level) = 'J&J';
                        
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_max') }} WHERE UPPER(level) = 'CATEGORY') AND UPPER(level) = 'CATEGORY';


                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_cd)),UPPER(TRIM(article_cd)),UPPER(TRIM(LEVEL))) 
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_code)),UPPER(TRIM(article_code)),UPPER(TRIM(LEVEL))
                        FROM {{ source('indsdl_raw', 'sdl_pos_vmm') }} WHERE UPPER(LEVEL) = 'J&J') AND UPPER(LEVEL) = 'J&J';
                    
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_vmm') }} WHERE UPPER(level) = 'CATEGORY') AND UPPER(level) = 'CATEGORY';


                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_cd)),UPPER(TRIM(article_cd)),UPPER(TRIM(LEVEL))) 
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_code)),UPPER(TRIM(article_code)),UPPER(TRIM(LEVEL))
                        FROM {{ source('indsdl_raw', 'sdl_pos_ril') }} WHERE UPPER(LEVEL) = 'J&J') AND UPPER(LEVEL) = 'J&J';
                    
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_ril') }} WHERE UPPER(level) = 'CATEGORY') AND UPPER(level) = 'CATEGORY';


                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_cd)),UPPER(TRIM(article_cd)),UPPER(TRIM(LEVEL))) 
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_code)),UPPER(TRIM(article_code)),UPPER(TRIM(LEVEL))
                        FROM {{ source('indsdl_raw', 'sdl_pos_abrl') }} WHERE UPPER(LEVEL) = 'J&J') AND UPPER(LEVEL) = 'J&J';
                    
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_abrl') }} WHERE UPPER(level) = 'CATEGORY') AND UPPER(level) = 'CATEGORY';


                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_cd)),UPPER(TRIM(article_cd)),UPPER(TRIM(LEVEL))) 
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_code)),UPPER(TRIM(article_code)),UPPER(TRIM(LEVEL))
                        FROM {{ source('indsdl_raw', 'sdl_pos_apollo') }} WHERE UPPER(LEVEL) = 'J&J') AND UPPER(LEVEL) = 'J&J';
                    
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_apollo') }} WHERE UPPER(level) = 'CATEGORY') AND UPPER(level) = 'CATEGORY';


                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_cd)),UPPER(TRIM(article_cd)),UPPER(TRIM(LEVEL)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_code)),UPPER(TRIM(article_code)),UPPER(TRIM(LEVEL))
                        FROM {{ source('indsdl_raw', 'sdl_pos_tesco') }} WHERE UPPER(LEVEL) = 'J&J') AND UPPER(LEVEL) = 'J&J';
                    
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_tesco') }} WHERE UPPER(level) = 'CATEGORY') AND UPPER(level) = 'CATEGORY';


                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_cd)),UPPER(TRIM(article_cd)),UPPER(TRIM(LEVEL))) 
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_code)),UPPER(TRIM(article_code)),UPPER(TRIM(LEVEL))
                        FROM {{ source('indsdl_raw', 'sdl_pos_dmart') }} WHERE UPPER(LEVEL) = 'J&J') AND UPPER(LEVEL) = 'J&J';
                    
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_dmart') }} WHERE UPPER(level) = 'CATEGORY') AND UPPER(level) = 'CATEGORY';


                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_cd)),UPPER(TRIM(article_cd)),UPPER(TRIM(LEVEL))) 
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_code)),UPPER(TRIM(article_code)),UPPER(TRIM(LEVEL))
                        FROM {{ source('indsdl_raw', 'sdl_pos_frl') }} WHERE UPPER(LEVEL) = 'J&J') AND UPPER(LEVEL) = 'J&J';
                    
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_frl') }} WHERE UPPER(level) = 'CATEGORY') AND UPPER(level) = 'CATEGORY';


                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_cd)),UPPER(TRIM(article_cd)),UPPER(TRIM(LEVEL))) 
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_code)),UPPER(TRIM(article_code)),UPPER(TRIM(LEVEL))
                        FROM {{ source('indsdl_raw', 'sdl_pos_dabur') }} WHERE UPPER(LEVEL) = 'J&J') AND UPPER(LEVEL) = 'J&J';
                    
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_dabur') }} WHERE UPPER(level) = 'CATEGORY') AND UPPER(level) = 'CATEGORY';


                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_cd)),UPPER(TRIM(article_cd)),UPPER(TRIM(LEVEL))) 
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_code)),UPPER(TRIM(article_code)),UPPER(TRIM(LEVEL))
                        FROM {{ source('indsdl_raw', 'sdl_pos_spencer') }} WHERE UPPER(LEVEL) = 'J&J') AND UPPER(LEVEL) = 'J&J';
                    
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_spencer') }} WHERE UPPER(level) = 'CATEGORY') AND UPPER(level) = 'CATEGORY';
                {% endif %}"
    )
}}
with sdl_pos_hg as
(
    select * from {{ source('indsdl_raw', 'sdl_pos_hg') }}
),
sdl_pos_max as
(
    select * from {{ source('indsdl_raw', 'sdl_pos_max') }}
),
sdl_pos_vmm as
(
    select * from {{ source('indsdl_raw', 'sdl_pos_vmm') }}
),
sdl_pos_ril as
(
    select * from {{ source('indsdl_raw', 'sdl_pos_ril') }}
),
sdl_pos_abrl as
(
    select * from {{ source('indsdl_raw', 'sdl_pos_abrl') }}
),
sdl_pos_apollo as
(
    select * from {{ source('indsdl_raw', 'sdl_pos_apollo') }}
),
sdl_pos_tesco as
(
    select * from {{ source('indsdl_raw', 'sdl_pos_tesco') }}
),
sdl_pos_dmart as
(
    select * from {{ source('indsdl_raw', 'sdl_pos_dmart') }}
),
sdl_pos_frl as
(
    select * from {{ source('indsdl_raw', 'sdl_pos_frl') }}
),
sdl_pos_dabur as
(
    select * from {{ source('indsdl_raw', 'sdl_pos_dabur') }}
),
sdl_pos_spencer as
(
    select * from {{ source('indsdl_raw', 'sdl_pos_spencer') }}
),

hg as
(
    select
        upper(key_account_name)::varchar(255) as key_account_name,
        pos_dt::date as pos_dt,
        store_code::varchar(20) as store_cd,
        subcategory::varchar(255) as subcategory,
        level::varchar(10) as level,
        article_code::varchar(20) as article_cd,
        sls_qty::number(38,6) as sls_qty,
        sls_val_lcy::number(38,6) as sls_val_lcy,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        filename::varchar(255) as source_file_name,
        file_upload_date::date as file_upload_date,
        run_id::number(14,0) as run_id
    from sdl_pos_hg
    WHERE UPPER(level) = 'J&J'
),
hg1 as
(
    select
        upper(key_account_name)::varchar(255) as key_account_name,
        pos_dt::date as pos_dt,
        store_code::varchar(20) as store_cd,
        subcategory::varchar(255) as subcategory,
        level::varchar(10) as level,
        article_code::varchar(20) as article_cd,
        sls_qty::number(38,6) as sls_qty,
        sls_val_lcy::number(38,6) as sls_val_lcy,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        filename::varchar(255) as source_file_name,
        file_upload_date::date as file_upload_date,
        run_id::number(14,0) as run_id
    from sdl_pos_hg
    WHERE UPPER(LEVEL) = 'CATEGORY'
),

max as
(
    select
        upper(key_account_name)::varchar(255) as key_account_name,
        pos_dt::date as pos_dt,
        store_code::varchar(20) as store_cd,
        subcategory::varchar(255) as subcategory,
        level::varchar(10) as level,
        article_code::varchar(20) as article_cd,
        sls_qty::number(38,6) as sls_qty,
        sls_val_lcy::number(38,6) as sls_val_lcy,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        filename::varchar(255) as source_file_name,
        file_upload_date::date as file_upload_date,
        run_id::number(14,0) as run_id
    from sdl_pos_max
    WHERE UPPER(level) = 'J&J'
),
max1 as
(
    select
        upper(key_account_name)::varchar(255) as key_account_name,
        pos_dt::date as pos_dt,
        store_code::varchar(20) as store_cd,
        subcategory::varchar(255) as subcategory,
        level::varchar(10) as level,
        article_code::varchar(20) as article_cd,
        sls_qty::number(38,6) as sls_qty,
        sls_val_lcy::number(38,6) as sls_val_lcy,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        filename::varchar(255) as source_file_name,
        file_upload_date::date as file_upload_date,
        run_id::number(14,0) as run_id
    from sdl_pos_max
    WHERE UPPER(LEVEL) = 'CATEGORY'
),

vmm as
(
    select
        upper(key_account_name)::varchar(255) as key_account_name,
        pos_dt::date as pos_dt,
        store_code::varchar(20) as store_cd,
        subcategory::varchar(255) as subcategory,
        level::varchar(10) as level,
        article_code::varchar(20) as article_cd,
        sls_qty::number(38,6) as sls_qty,
        sls_val_lcy::number(38,6) as sls_val_lcy,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        filename::varchar(255) as source_file_name,
        file_upload_date::date as file_upload_date,
        run_id::number(14,0) as run_id
    from sdl_pos_vmm
    WHERE UPPER(level) = 'J&J'
),
vmm1 as
(
    select
        upper(key_account_name)::varchar(255) as key_account_name,
        pos_dt::date as pos_dt,
        store_code::varchar(20) as store_cd,
        subcategory::varchar(255) as subcategory,
        level::varchar(10) as level,
        article_code::varchar(20) as article_cd,
        sls_qty::number(38,6) as sls_qty,
        sls_val_lcy::number(38,6) as sls_val_lcy,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        filename::varchar(255) as source_file_name,
        file_upload_date::date as file_upload_date,
        run_id::number(14,0) as run_id
    from sdl_pos_vmm
    WHERE UPPER(LEVEL) = 'CATEGORY'
),

ril as
(
    select
        upper(key_account_name)::varchar(255) as key_account_name,
        pos_dt::date as pos_dt,
        store_code::varchar(20) as store_cd,
        subcategory::varchar(255) as subcategory,
        level::varchar(10) as level,
        article_code::varchar(20) as article_cd,
        sls_qty::number(38,6) as sls_qty,
        sls_val_lcy::number(38,6) as sls_val_lcy,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        filename::varchar(255) as source_file_name,
        file_upload_date::date as file_upload_date,
        run_id::number(14,0) as run_id
    from sdl_pos_ril
    WHERE UPPER(level) = 'J&J'
),
ril1 as
(
    select
        upper(key_account_name)::varchar(255) as key_account_name,
        pos_dt::date as pos_dt,
        store_code::varchar(20) as store_cd,
        subcategory::varchar(255) as subcategory,
        level::varchar(10) as level,
        article_code::varchar(20) as article_cd,
        sls_qty::number(38,6) as sls_qty,
        sls_val_lcy::number(38,6) as sls_val_lcy,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        filename::varchar(255) as source_file_name,
        file_upload_date::date as file_upload_date,
        run_id::number(14,0) as run_id
    from sdl_pos_ril
    WHERE UPPER(LEVEL) = 'CATEGORY'
),

abrl as
(
    select
        upper(key_account_name)::varchar(255) as key_account_name,
        pos_dt::date as pos_dt,
        store_code::varchar(20) as store_cd,
        subcategory::varchar(255) as subcategory,
        level::varchar(10) as level,
        article_code::varchar(20) as article_cd,
        sls_qty::number(38,6) as sls_qty,
        sls_val_lcy::number(38,6) as sls_val_lcy,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        filename::varchar(255) as source_file_name,
        file_upload_date::date as file_upload_date,
        run_id::number(14,0) as run_id
    from sdl_pos_abrl
    WHERE UPPER(level) = 'J&J'
),
abrl1 as
(
    select
        upper(key_account_name)::varchar(255) as key_account_name,
        pos_dt::date as pos_dt,
        store_code::varchar(20) as store_cd,
        subcategory::varchar(255) as subcategory,
        level::varchar(10) as level,
        article_code::varchar(20) as article_cd,
        sls_qty::number(38,6) as sls_qty,
        sls_val_lcy::number(38,6) as sls_val_lcy,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        filename::varchar(255) as source_file_name,
        file_upload_date::date as file_upload_date,
        run_id::number(14,0) as run_id
    from sdl_pos_abrl
    WHERE UPPER(LEVEL) = 'CATEGORY'
),

apollo as
(
    select
        upper(key_account_name)::varchar(255) as key_account_name,
        pos_dt::date as pos_dt,
        store_code::varchar(20) as store_cd,
        subcategory::varchar(255) as subcategory,
        level::varchar(10) as level,
        article_code::varchar(20) as article_cd,
        sls_qty::number(38,6) as sls_qty,
        sls_val_lcy::number(38,6) as sls_val_lcy,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        filename::varchar(255) as source_file_name,
        file_upload_date::date as file_upload_date,
        run_id::number(14,0) as run_id
    from sdl_pos_apollo
    WHERE UPPER(level) = 'J&J'
),
apollo1 as
(
    select
        upper(key_account_name)::varchar(255) as key_account_name,
        pos_dt::date as pos_dt,
        store_code::varchar(20) as store_cd,
        subcategory::varchar(255) as subcategory,
        level::varchar(10) as level,
        article_code::varchar(20) as article_cd,
        sls_qty::number(38,6) as sls_qty,
        sls_val_lcy::number(38,6) as sls_val_lcy,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        filename::varchar(255) as source_file_name,
        file_upload_date::date as file_upload_date,
        run_id::number(14,0) as run_id
    from sdl_pos_apollo
    WHERE UPPER(LEVEL) = 'CATEGORY'
),

tesco as
(
    select
        upper(key_account_name)::varchar(255) as key_account_name,
        pos_dt::date as pos_dt,
        store_code::varchar(20) as store_cd,
        subcategory::varchar(255) as subcategory,
        level::varchar(10) as level,
        article_code::varchar(20) as article_cd,
        sls_qty::number(38,6) as sls_qty,
        sls_val_lcy::number(38,6) as sls_val_lcy,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        filename::varchar(255) as source_file_name,
        file_upload_date::date as file_upload_date,
        run_id::number(14,0) as run_id
    from sdl_pos_tesco
    WHERE UPPER(level) = 'J&J'
),
tesco1 as
(
    select
        upper(key_account_name)::varchar(255) as key_account_name,
        pos_dt::date as pos_dt,
        store_code::varchar(20) as store_cd,
        subcategory::varchar(255) as subcategory,
        level::varchar(10) as level,
        article_code::varchar(20) as article_cd,
        sls_qty::number(38,6) as sls_qty,
        sls_val_lcy::number(38,6) as sls_val_lcy,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        filename::varchar(255) as source_file_name,
        file_upload_date::date as file_upload_date,
        run_id::number(14,0) as run_id
    from sdl_pos_tesco
    WHERE UPPER(LEVEL) = 'CATEGORY'
),

dmart as
(
    select
        upper(key_account_name)::varchar(255) as key_account_name,
        pos_dt::date as pos_dt,
        store_code::varchar(20) as store_cd,
        subcategory::varchar(255) as subcategory,
        level::varchar(10) as level,
        article_code::varchar(20) as article_cd,
        sls_qty::number(38,6) as sls_qty,
        sls_val_lcy::number(38,6) as sls_val_lcy,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        filename::varchar(255) as source_file_name,
        file_upload_date::date as file_upload_date,
        run_id::number(14,0) as run_id
    from sdl_pos_dmart
    WHERE UPPER(level) = 'J&J'
),
dmart1 as
(
    select
        upper(key_account_name)::varchar(255) as key_account_name,
        pos_dt::date as pos_dt,
        store_code::varchar(20) as store_cd,
        subcategory::varchar(255) as subcategory,
        level::varchar(10) as level,
        article_code::varchar(20) as article_cd,
        sls_qty::number(38,6) as sls_qty,
        sls_val_lcy::number(38,6) as sls_val_lcy,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        filename::varchar(255) as source_file_name,
        file_upload_date::date as file_upload_date,
        run_id::number(14,0) as run_id
    from sdl_pos_dmart
    WHERE UPPER(LEVEL) = 'CATEGORY'
),

frl as
(
    select
        upper(key_account_name)::varchar(255) as key_account_name,
        pos_dt::date as pos_dt,
        store_code::varchar(20) as store_cd,
        subcategory::varchar(255) as subcategory,
        level::varchar(10) as level,
        article_code::varchar(20) as article_cd,
        sls_qty::number(38,6) as sls_qty,
        sls_val_lcy::number(38,6) as sls_val_lcy,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        filename::varchar(255) as source_file_name,
        file_upload_date::date as file_upload_date,
        run_id::number(14,0) as run_id
    from sdl_pos_frl
    WHERE UPPER(level) = 'J&J'
),
frl1 as
(
    select
        upper(key_account_name)::varchar(255) as key_account_name,
        pos_dt::date as pos_dt,
        store_code::varchar(20) as store_cd,
        subcategory::varchar(255) as subcategory,
        level::varchar(10) as level,
        article_code::varchar(20) as article_cd,
        sls_qty::number(38,6) as sls_qty,
        sls_val_lcy::number(38,6) as sls_val_lcy,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        filename::varchar(255) as source_file_name,
        file_upload_date::date as file_upload_date,
        run_id::number(14,0) as run_id
    from sdl_pos_frl
    WHERE UPPER(LEVEL) = 'CATEGORY'
),

dabur as
(
    select
        upper(key_account_name)::varchar(255) as key_account_name,
        pos_dt::date as pos_dt,
        store_code::varchar(20) as store_cd,
        subcategory::varchar(255) as subcategory,
        level::varchar(10) as level,
        article_code::varchar(20) as article_cd,
        sls_qty::number(38,6) as sls_qty,
        sls_val_lcy::number(38,6) as sls_val_lcy,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        filename::varchar(255) as source_file_name,
        file_upload_date::date as file_upload_date,
        run_id::number(14,0) as run_id
    from sdl_pos_dabur
    WHERE UPPER(level) = 'J&J'
),
dabur1 as
(
    select
        upper(key_account_name)::varchar(255) as key_account_name,
        pos_dt::date as pos_dt,
        store_code::varchar(20) as store_cd,
        subcategory::varchar(255) as subcategory,
        level::varchar(10) as level,
        article_code::varchar(20) as article_cd,
        sls_qty::number(38,6) as sls_qty,
        sls_val_lcy::number(38,6) as sls_val_lcy,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        filename::varchar(255) as source_file_name,
        file_upload_date::date as file_upload_date,
        run_id::number(14,0) as run_id
    from sdl_pos_dabur
    WHERE UPPER(LEVEL) = 'CATEGORY'
),

spencer as
(
    select
        upper(key_account_name)::varchar(255) as key_account_name,
        pos_dt::date as pos_dt,
        store_code::varchar(20) as store_cd,
        subcategory::varchar(255) as subcategory,
        level::varchar(10) as level,
        article_code::varchar(20) as article_cd,
        sls_qty::number(38,6) as sls_qty,
        sls_val_lcy::number(38,6) as sls_val_lcy,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        filename::varchar(255) as source_file_name,
        file_upload_date::date as file_upload_date,
        run_id::number(14,0) as run_id
    from sdl_pos_spencer
    WHERE UPPER(level) = 'J&J'
),
spencer1 as
(
    select
        upper(key_account_name)::varchar(255) as key_account_name,
        pos_dt::date as pos_dt,
        store_code::varchar(20) as store_cd,
        subcategory::varchar(255) as subcategory,
        level::varchar(10) as level,
        article_code::varchar(20) as article_cd,
        sls_qty::number(38,6) as sls_qty,
        sls_val_lcy::number(38,6) as sls_val_lcy,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        filename::varchar(255) as source_file_name,
        file_upload_date::date as file_upload_date,
        run_id::number(14,0) as run_id
    from sdl_pos_spencer
    WHERE UPPER(LEVEL) = 'CATEGORY'
),
transformed as 
(
    select * from hg
    union all
    select * from hg1
    union all
    select * from max
    union all
    select * from max1
    union all
    select * from vmm
    union all
    select * from vmm1
    union all
    select * from ril
    union all
    select * from ril1
    union all
    select * from abrl
    union all
    select * from abrl1
    union all
    select * from apollo
    union all
    select * from apollo1
    union all
    select * from tesco
    union all
    select * from tesco1
    union all
    select * from dmart
    union all
    select * from dmart1
    union all
    select * from frl
    union all
    select * from frl1
    union all
    select * from dabur
    union all
    select * from dabur1
    union all
    select * from spencer
    union all
    select * from spencer1
)
select * from transformed
