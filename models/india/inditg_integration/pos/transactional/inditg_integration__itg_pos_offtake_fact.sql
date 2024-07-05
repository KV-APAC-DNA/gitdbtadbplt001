{{
    config
    (
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="{% if is_incremental() %}
                    {% if var('pos_job_to_execute') == 'pos_hg_files' %}
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_cd)),UPPER(TRIM(article_cd)),UPPER(TRIM(level))) 
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_code)),UPPER(TRIM(article_code)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_hg') }} WHERE UPPER(level) = 'J&J') AND UPPER(level) = 'J&J';
                        
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_hg') }} WHERE UPPER(level) = 'CATEGORY') AND UPPER(level) = 'CATEGORY';
                    
                    {% elif var('pos_job_to_execute') == 'pos_max_files' %}
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_cd)),UPPER(TRIM(article_cd)),UPPER(TRIM(level)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_code)),UPPER(TRIM(article_code)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_max') }} WHERE UPPER(level) = 'J&J') AND UPPER(level) = 'J&J';
                        
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_max') }} WHERE UPPER(level) = 'CATEGORY') AND UPPER(level) = 'CATEGORY';

                    {% elif var('pos_job_to_execute') == 'pos_vmm_files' %}
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_cd)),UPPER(TRIM(article_cd)),UPPER(TRIM(LEVEL))) 
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_code)),UPPER(TRIM(article_code)),UPPER(TRIM(LEVEL))
                        FROM {{ source('indsdl_raw', 'sdl_pos_vmm') }} WHERE UPPER(LEVEL) = 'J&J') AND UPPER(LEVEL) = 'J&J';
                    
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_vmm') }} WHERE UPPER(level) = 'CATEGORY') AND UPPER(level) = 'CATEGORY';

                    {% elif var('pos_job_to_execute') == 'pos_ril_files' %}
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_cd)),UPPER(TRIM(article_cd)),UPPER(TRIM(LEVEL))) 
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_code)),UPPER(TRIM(article_code)),UPPER(TRIM(LEVEL))
                        FROM {{ source('indsdl_raw', 'sdl_pos_ril') }} WHERE UPPER(LEVEL) = 'J&J') AND UPPER(LEVEL) = 'J&J';
                    
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_ril') }} WHERE UPPER(level) = 'CATEGORY') AND UPPER(level) = 'CATEGORY';

                    {% elif var('pos_job_to_execute') == 'pos_abrl_files' %}
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_cd)),UPPER(TRIM(article_cd)),UPPER(TRIM(LEVEL))) 
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_code)),UPPER(TRIM(article_code)),UPPER(TRIM(LEVEL))
                        FROM {{ source('indsdl_raw', 'sdl_pos_abrl') }} WHERE UPPER(LEVEL) = 'J&J') AND UPPER(LEVEL) = 'J&J';
                    
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_abrl') }} WHERE UPPER(level) = 'CATEGORY') AND UPPER(level) = 'CATEGORY';

                    {% elif var('pos_job_to_execute') == 'pos_apollo_files' %}
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_cd)),UPPER(TRIM(article_cd)),UPPER(TRIM(LEVEL))) 
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_code)),UPPER(TRIM(article_code)),UPPER(TRIM(LEVEL))
                        FROM {{ source('indsdl_raw', 'sdl_pos_apollo') }} WHERE UPPER(LEVEL) = 'J&J') AND UPPER(LEVEL) = 'J&J';
                    
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_apollo') }} WHERE UPPER(level) = 'CATEGORY') AND UPPER(level) = 'CATEGORY';

                    {% elif var('pos_job_to_execute') == 'pos_tesco_files' %}
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_cd)),UPPER(TRIM(article_cd)),UPPER(TRIM(LEVEL)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_code)),UPPER(TRIM(article_code)),UPPER(TRIM(LEVEL))
                        FROM {{ source('indsdl_raw', 'sdl_pos_tesco') }} WHERE UPPER(LEVEL) = 'J&J') AND UPPER(LEVEL) = 'J&J';
                    
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_tesco') }} WHERE UPPER(level) = 'CATEGORY') AND UPPER(level) = 'CATEGORY';

                    {% elif var('pos_job_to_execute') == 'pos_dmart_files' %}
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_cd)),UPPER(TRIM(article_cd)),UPPER(TRIM(LEVEL))) 
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_code)),UPPER(TRIM(article_code)),UPPER(TRIM(LEVEL))
                        FROM {{ source('indsdl_raw', 'sdl_pos_dmart') }} WHERE UPPER(LEVEL) = 'J&J') AND UPPER(LEVEL) = 'J&J';
                    
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_dmart') }} WHERE UPPER(level) = 'CATEGORY') AND UPPER(level) = 'CATEGORY';

                    {% elif var('pos_job_to_execute') == 'pos_frl_files' %}
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_cd)),UPPER(TRIM(article_cd)),UPPER(TRIM(LEVEL))) 
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_code)),UPPER(TRIM(article_code)),UPPER(TRIM(LEVEL))
                        FROM {{ source('indsdl_raw', 'sdl_pos_frl') }} WHERE UPPER(LEVEL) = 'J&J') AND UPPER(LEVEL) = 'J&J';
                    
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_frl') }} WHERE UPPER(level) = 'CATEGORY') AND UPPER(level) = 'CATEGORY';

                    {% elif var('pos_job_to_execute') == 'pos_dabur_files' %}
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_cd)),UPPER(TRIM(article_cd)),UPPER(TRIM(LEVEL))) 
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_code)),UPPER(TRIM(article_code)),UPPER(TRIM(LEVEL))
                        FROM {{ source('indsdl_raw', 'sdl_pos_dabur') }} WHERE UPPER(LEVEL) = 'J&J') AND UPPER(LEVEL) = 'J&J';
                    
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_dabur') }} WHERE UPPER(level) = 'CATEGORY') AND UPPER(level) = 'CATEGORY';

                    {% elif var('pos_job_to_execute') == 'pos_spencer_files' %}
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_cd)),UPPER(TRIM(article_cd)),UPPER(TRIM(LEVEL))) 
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(store_code)),UPPER(TRIM(article_code)),UPPER(TRIM(LEVEL))
                        FROM {{ source('indsdl_raw', 'sdl_pos_spencer') }} WHERE UPPER(LEVEL) = 'J&J') AND UPPER(LEVEL) = 'J&J';
                    
                        delete from {{this}} WHERE (UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level)))
                        IN (SELECT DISTINCT UPPER(TRIM(key_account_name)),pos_dt,UPPER(TRIM(subcategory)),UPPER(TRIM(level))
                        FROM {{ source('indsdl_raw', 'sdl_pos_spencer') }} WHERE UPPER(level) = 'CATEGORY') AND UPPER(level) = 'CATEGORY';

                    {% endif %}
                {% endif %}"
    )
}}
with source as
(
    select * from {{ source('indsdl_raw', 'sdl_pos_hg') }}
),
source2 as
(
    select * from {{ source('indsdl_raw', 'sdl_pos_max') }}
),
source3 as
(
    select * from {{ source('indsdl_raw', 'sdl_pos_vmm') }}
),
source4 as
(
    select * from {{ source('indsdl_raw', 'sdl_pos_ril') }}
),
source5 as
(
    select * from {{ source('indsdl_raw', 'sdl_pos_abrl') }}
),
source6 as
(
    select * from {{ source('indsdl_raw', 'sdl_pos_apollo') }}
),
source7 as
(
    select * from {{ source('indsdl_raw', 'sdl_pos_tesco') }}
),
source8 as
(
    select * from {{ source('indsdl_raw', 'sdl_pos_dmart') }}
),
source9 as
(
    select * from {{ source('indsdl_raw', 'sdl_pos_frl') }}
),
source10 as
(
    select * from {{ source('indsdl_raw', 'sdl_pos_dabur') }}
),
source11 as
(
    select * from {{ source('indsdl_raw', 'sdl_pos_spencer') }}
),

{% if var("pos_job_to_execute") == 'pos_hg_files' %}

final as
(
    select
        key_account_name::varchar(255) as key_account_name,
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
    from source
    WHERE UPPER(level) = 'J&J'
),
final1 as
(
    select
        key_account_name::varchar(255) as key_account_name,
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
    from source
    WHERE UPPER(LEVEL) = 'CATEGORY'
),
transformed as 
(
    select * from final
    union all
    select * from final1
)
select * from transformed

{% elif var("pos_job_to_execute") == 'pos_max_files' %}

final as
(
    select
        key_account_name::varchar(255) as key_account_name,
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
    from source2
    WHERE UPPER(level) = 'J&J'
),
final1 as
(
    select
        key_account_name::varchar(255) as key_account_name,
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
    from source2
    WHERE UPPER(LEVEL) = 'CATEGORY'
),
transformed as 
(
    select * from final
    union all
    select * from final1
)
select * from transformed

{% elif var("pos_job_to_execute") == 'pos_vmm_files' %}

final as
(
    select
        key_account_name::varchar(255) as key_account_name,
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
    from source3
    WHERE UPPER(level) = 'J&J'
),
final1 as
(
    select
        key_account_name::varchar(255) as key_account_name,
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
    from source3
    WHERE UPPER(LEVEL) = 'CATEGORY'
),
transformed as 
(
    select * from final
    union all
    select * from final1
)
select * from transformed

{% elif var("pos_job_to_execute") == 'pos_ril_files' %}

final as
(
    select
        key_account_name::varchar(255) as key_account_name,
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
    from source4
    WHERE UPPER(level) = 'J&J'
),
final1 as
(
    select
        key_account_name::varchar(255) as key_account_name,
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
    from source4
    WHERE UPPER(LEVEL) = 'CATEGORY'
),
transformed as 
(
    select * from final
    union all
    select * from final1
)
select * from transformed

{% elif var("pos_job_to_execute") == 'pos_abrl_files' %}

final as
(
    select
        key_account_name::varchar(255) as key_account_name,
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
    from source5
    WHERE UPPER(level) = 'J&J'
),
final1 as
(
    select
        key_account_name::varchar(255) as key_account_name,
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
    from source5
    WHERE UPPER(LEVEL) = 'CATEGORY'
),
transformed as 
(
    select * from final
    union all
    select * from final1
)
select * from transformed

{% elif var("pos_job_to_execute") == 'pos_apollo_files' %}

final as
(
    select
        key_account_name::varchar(255) as key_account_name,
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
    from source6
    WHERE UPPER(level) = 'J&J'
),
final1 as
(
    select
        key_account_name::varchar(255) as key_account_name,
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
    from source6
    WHERE UPPER(LEVEL) = 'CATEGORY'
),
transformed as 
(
    select * from final
    union all
    select * from final1
)
select * from transformed

{% elif var("pos_job_to_execute") == 'pos_tesco_files' %}

final as
(
    select
        key_account_name::varchar(255) as key_account_name,
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
    from source7
    WHERE UPPER(level) = 'J&J'
),
final1 as
(
    select
        key_account_name::varchar(255) as key_account_name,
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
    from source7
    WHERE UPPER(LEVEL) = 'CATEGORY'
),
transformed as 
(
    select * from final
    union all
    select * from final1
)
select * from transformed

{% elif var("pos_job_to_execute") == 'pos_dmart_files' %}

final as
(
    select
        key_account_name::varchar(255) as key_account_name,
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
    from source8
    WHERE UPPER(level) = 'J&J'
),
final1 as
(
    select
        key_account_name::varchar(255) as key_account_name,
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
    from source8
    WHERE UPPER(LEVEL) = 'CATEGORY'
),
transformed as 
(
    select * from final
    union all
    select * from final1
)
select * from transformed

{% elif var("pos_job_to_execute") == 'pos_frl_files' %}

final as
(
    select
        key_account_name::varchar(255) as key_account_name,
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
    from source9
    WHERE UPPER(level) = 'J&J'
),
final1 as
(
    select
        key_account_name::varchar(255) as key_account_name,
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
    from source9
    WHERE UPPER(LEVEL) = 'CATEGORY'
),
transformed as 
(
    select * from final
    union all
    select * from final1
)
select * from transformed

{% elif var("pos_job_to_execute") == 'pos_dabur_files' %}

final as
(
    select
        key_account_name::varchar(255) as key_account_name,
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
    from source10
    WHERE UPPER(level) = 'J&J'
),
final1 as
(
    select
        key_account_name::varchar(255) as key_account_name,
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
    from source10
    WHERE UPPER(LEVEL) = 'CATEGORY'
),
transformed as 
(
    select * from final
    union all
    select * from final1
)
select * from transformed

{% elif var("pos_job_to_execute") == 'pos_spencer_files' %}

final as
(
    select
        key_account_name::varchar(255) as key_account_name,
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
    from source11
    WHERE UPPER(level) = 'J&J'
),
final1 as
(
    select
        key_account_name::varchar(255) as key_account_name,
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
    from source11
    WHERE UPPER(LEVEL) = 'CATEGORY'
),
transformed as 
(
    select * from final
    union all
    select * from final1
)
select * from transformed

{% endif %}
