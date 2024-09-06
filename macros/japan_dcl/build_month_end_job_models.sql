{% macro build_month_end_job_models(model) %}
{% if execute %}
        {% if target.name=='prod' %}
            {% set source_table %}
                    jpdcledw_integration.cld_m
            {% endset %}
        {% else %}
            {% set source_table %}
                jpndcledw_integration__cld_m
            {% endset %}
       
        {% endif %}
        {% set count_query %}
            select count(*) as cnt
                from {{ source_table }}
                where to_date(ymd_dt) = to_date(CONVERT_TIMEZONE('Asia/Tokyo', current_timestamp()))
                    and day_of_week = 'Mon'
                    and month_445 <> (
                        select month_445
                        from {{ source_table }}
                        where to_date(ymd_dt) = to_date(CONVERT_TIMEZONE( 'Asia/Tokyo', current_timestamp()))-1
                    )
        {% endset %}

    {% set result = run_query(count_query) %}
    {% set count = result.columns[0].values()[0] %}
    
    {% if count != 0 %}
        {{ return(True) }}
    {% else %}
        {{ return(False) }}
    {% endif %}
{% endif %}
{% endmacro %}
