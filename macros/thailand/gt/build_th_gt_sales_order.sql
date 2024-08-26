{% macro build_th_gt_sales_order() %}
    {{ log("===============================================================================================") }}
    {{ log("Step1: Trying to fetch the filenames from SDL table: thasdl_raw.sdl_th_gt_sales_order ") }}
    {{ log("===============================================================================================") }}
    {{ log("Trying to set the query to fetch the file names ") }}
    {{ log("===============================================================================================") }}
    {% set get_file_names_query %}
        select filename from {{ source('thasdl_raw', 'sdl_th_gt_sales_order') }}
        where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_sales_order__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_sales_order__duplicate_test') }}
            union all 
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_sales_order__test_format') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_sales_order__test_date_format_odd_eve_leap') }}
            )
        group by filename
        order by filename asc;
    {% endset %}
    {{ log("===============================================================================================") }}
    {{ log("Set the query to fetch the file names ") }}
    {{ log("===============================================================================================") }}
    {{ log("Try to execute the query to fetch the file names ") }}
    {{ log("===============================================================================================") }}
        {% set get_file_names_query_result = run_query(get_file_names_query) %}
            {% if execute %}
                {% set filenames = get_file_names_query_result.columns[0].values() %}
            {% else %}
                {% set filenames = [] %}
            {% endif %}
        {% for file in filenames %}
            {{log(file)}}
            {{log("--------------")}}
        {% endfor %}
    {{ log("===============================================================================================") }}
    {{ log("Fetched the filenames from SDL table: thasdl_raw.sdl_th_gt_sales_order ") }}
    {{ log("===============================================================================================") }}
    {{ log("Started building preload table for file: "~ file) }}
    {% for file in filenames %}
        {{ log("===============================================================================================") }}
        {{ log("Started building pre_load table for file: "~ file) }}
        {{ log("===============================================================================================") }}
        {{build_wks_th_gt_sales_order_pre_load(file)}}
        {{ log("===============================================================================================") }}
        {{ log("Completed building pre_load table for file: "~ file) }}
        {{ log("===============================================================================================") }}
        {{ log("Started building flag_incl table for file: "~ file) }}
        {{build_wks_th_gt_sales_order_flag_incl(file)}}
        {{ log("Completed building flag_incl table for file: "~ file) }}
        {{ log("===============================================================================================") }}
        {{ log("Started building itg table for file: "~ file) }}
        {{ log("===============================================================================================") }}
        {{build_itg_th_gt_sales_order(file)}}
        {{ log("===============================================================================================") }}
        {{ log("Completed building itg table for file: "~ file) }}
        {{ log("===============================================================================================") }}
    {% endfor %}
{% endmacro %}
