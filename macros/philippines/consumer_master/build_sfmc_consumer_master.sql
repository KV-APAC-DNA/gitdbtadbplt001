{% macro build_sfmc_consumer_master() %}
    {{ log("===============================================================================================") }}
    {{ log("Step1: Trying to fetch the filenames from SDL table: phlsdl_raw.sdl_ph_sfmc_consumer_master ") }}
    {{ log("===============================================================================================") }}
    {{ log("Trying to set the query to fetch the file names ") }}
    {{ log("===============================================================================================") }}
    {% set get_file_names_query %}
        select file_name from {{ source('phlsdl_raw', 'sdl_ph_sfmc_consumer_master') }}
        where file_name not in (
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_sfmc_consumer_master__test_null__ff')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_sfmc_consumer_master__test_duplicate__ff')}}
    )
        group by file_name
        order by file_name asc;
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

        {{ log("===============================================================================================") }}
        {{ log("Fetched the filenames from SDL table: phlsdl_raw.sdl_ph_sfmc_consumer_master "~ filenames) }}
        {{ log("===============================================================================================") }}

    {{ log("Started building itg_temp table for file: "~ file) }}
    {% for file in filenames %}
        {{ log("===============================================================================================") }}
        {{ log("Started building itg_temp table for file: "~ file) }}
        {{ log("===============================================================================================") }}
        {{build_itg_sfmc_consumer_master_temp(file)}}
        {{ log("===============================================================================================") }}
        {{ log("Completed building itg_temp table for file: "~ file) }}
        {{ log("===============================================================================================") }}
        {{ log("Started building wks_temp table for file: "~ file) }}
        {{build_wks_ph_sfmc_consumer_master_temp1(file)}}
        {{ log("Completed building wks_temp table for file: "~ file) }}
        {{ log("===============================================================================================") }}
        {{ log("Started building wks_itg table for file: "~ file) }}
        {{ log("===============================================================================================") }}
        {{build_wks_itg_sfmc_consumer_master(file)}}
        {{ log("===============================================================================================") }}
        {{ log("Completed building wks_itg table for file: "~ file) }}
        {{ log("===============================================================================================") }}
    {% endfor %}
{% endmacro %}
