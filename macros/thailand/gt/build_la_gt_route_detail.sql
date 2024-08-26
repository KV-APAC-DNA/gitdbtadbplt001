{% macro build_la_gt_route_detail() %}
    {{ log("===============================================================================================") }}
    {{ log("Step1: Trying to fetch the filenames from SDL table: thasdl_raw.sdl_la_gt_route_detail ") }}
    {{ log("===============================================================================================") }}
    {{ log("Trying to set the query to fetch the file names ") }}
    {{ log("===============================================================================================") }}
    {% set get_file_names_query %}
        select filename from {{ source('thasdl_raw', 'sdl_la_gt_route_detail') }}
        where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_route_detail__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_route_detail__duplicate_test') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_route_detail__test_format_created_date') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_route_detail__test_format_sale_unit') }}
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
            {{file}}
        {% endfor %}
        {{ log("===============================================================================================") }}
        {{ log("Fetched the filenames from SDL table: thasdl_raw.sdl_la_gt_route_detail ") }}
        {{ log("===============================================================================================") }}
        {{ log("Started building preload table for file: "~ file) }}
    {% for file in filenames %}
        {{ log("===============================================================================================") }}
        {{ log("Started building hashkey table for file: "~ file) }}
        {{ log("===============================================================================================") }}
        {{build_wks_la_gt_route_detail_hashkey(file)}}
        {{ log("===============================================================================================") }}
        {{ log("Completed building hashkey table for file: "~ file) }}
        {{ log("===============================================================================================") }}
        {{ log("Started building pre_load table for file: "~ file) }}
        {{build_wks_la_gt_route_detail_pre_load(file)}}
        {{ log("Completed building pre_load table for file: "~ file) }}
        {{ log("===============================================================================================") }}
        {{ log("Started building itg table for file: "~ file) }}
        {{ log("===============================================================================================") }}
        {{build_itg_la_gt_route_detail(file)}}
        {{ log("===============================================================================================") }}
        {{ log("Completed building itg table for file: "~ file) }}
        {{ log("===============================================================================================") }}
    {% endfor %}
{% endmacro %}