{% macro build_cbd_gt_sales_report_fact() %}
    {{ log("===============================================================================================") }}
    {{ log("Step1: Trying to fetch the filenames from SDL table: thasdl_raw.sdl_cbd_gt_sales_report_fact ") }}
    {{ log("===============================================================================================") }}
    {{ log("Trying to set the query to fetch the file names ") }}
    {{ log("===============================================================================================") }}
    {% set get_file_names_query %}
        select filename from {{ source('thasdl_raw', 'sdl_cbd_gt_sales_report_fact') }}
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
            {{file}}
        {% endfor %}
        {{ log("===============================================================================================") }}
        {{ log("Fetched the filenames from SDL table: thasdl_raw.sdl_cbd_gt_sales_report_fact ") }}
        {{ log("===============================================================================================") }}
        
    {{ log("Started building preload table for file: "~ file) }}
    {% for file in filenames %}
        {{ log("===============================================================================================") }}
        {{ log("Started building preload table for file: "~ file) }}
        {{ log("===============================================================================================") }}
        {{build_wks_cbd_gt_sales_report_fact_pre_load(file)}}
        {{ log("===============================================================================================") }}
        {{ log("Completed building preload table for file: "~ file) }}
        {{ log("===============================================================================================") }}
        {{ log("Started building flag_incl table for file: "~ file) }}
        {{build_wks_cbd_gt_sales_report_fact_flag_incl(file)}}
        {{ log("Completed building flag_incl table for file: "~ file) }}
        {{ log("===============================================================================================") }}
        {{ log("Started building itg table for file: "~ file) }}
        {{ log("===============================================================================================") }}
        {{build_itg_cbd_gt_sales_report_fact(file)}}
        {{ log("===============================================================================================") }}
        {{ log("Completed building itg table for file: "~ file) }}
        {{ log("===============================================================================================") }}
    {% endfor %}
{% endmacro %}