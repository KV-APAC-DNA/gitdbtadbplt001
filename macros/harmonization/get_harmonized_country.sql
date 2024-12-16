{% macro get_harmonized_country(data_source, fresh_run_flag) %}
    {# Log data source and fresh run flag #}
    {{ log("Data Source: " ~ data_source) }}
    {{ log("Fresh Run Flag: " ~ fresh_run_flag) }}

    {% set sql_table %}
        SELECT
        distinct TABLENAME,FILTER_CONDITION
        FROM {{ target.database }}.inditg_integration.t_harmonization_rule_mstr
        WHERE
        UPPER(SRC_NAME) = UPPER('{{ data_source }}')
        AND UPPER(HRM_TYPE) = 'COUNTRY'
        AND ACTIVE_FLAG = 'Y'
    {% endset %}

    {% set update_unharmonized = run_query(sql_table) %}
    {% for source_table in update_unharmonized %}
        {{ log("table to update unharmonized: " ~ source_table.TABLENAME) }}
       {# update condition for data when run GHM #}
        {% set UPDT_NULL_QRY_1 %}
            UPDATE {{ source_table.TABLENAME }}
            SET GCGH_COUNTRY = NULL, GCGH_REGION = NULL, COUNTRY_HARMONIZED_BY = NULL
            WHERE lower(GCGH_COUNTRY) = 'unharmonized'
        {% endset %}
        {% do run_query(UPDT_NULL_QRY_1) %}

        {#update null if flage is Y#}
        {% set UPDT_NULL_QRY %}
            UPDATE {{ source_table.TABLENAME }} A
            SET GCGH_COUNTRY = NULL, GCGH_REGION = NULL, COUNTRY_HARMONIZED_BY = NULL
            WHERE GCGH_COUNTRY IS NOT NULL
        {% endset %}

        {% if source_table.FILTER_CONDITION  !='' and source_table.FILTER_CONDITION is not none  %}
        {% set UPDT_NULL_QRY %}
		{{UPDT_NULL_QRY}} and {{source_table.FILTER_CONDITION}}
        {% endset %}
		{% endif %}

         {#update null for fresh flag is not null#}
        {{ log("First_run_flag is Y or not: " ~ fresh_run_flag) }}
        {% if fresh_run_flag =='Y' or fresh_run_flag =='y'  %}
        {{ log("Running null update with filter: " ~ UPDT_NULL_QRY) }}
        {% do run_query(UPDT_NULL_QRY) %}
        {% set fresh_run_flag = 'N'  %}
		{% endif %}

    {%endfor%}

    {# Log the generated SQL query
    {{ #log("Generated SQL: " ~ sql) }}
    #}
{# Define the SQL query to fetch harmonization rules #}
    {% set sql %}
        SELECT
            RULE_SEQ,
            CAST(REPLACE(RULE_SEQ, 'R', '') AS INTEGER) AS RULE_ID,
            UPPER(COLUMN_NAME) AS COLUMN_NAME,
            PATTERN_ID,
            TABLENAME,
            FILTER_CONDITION
        FROM
            {{ target.database }}.inditg_integration.t_harmonization_rule_mstr
        WHERE
            UPPER(SRC_NAME) = UPPER('{{ data_source }}')
            AND UPPER(HRM_TYPE) = 'COUNTRY'
            AND ACTIVE_FLAG = 'Y'
        ORDER BY RULE_ID
    {% endset %}
    {# Execute the SQL query and loop through the result set #}
    {% set harmonization_rules = run_query(sql) %}
    {% for rule in harmonization_rules %}
        {# Log rule details #}
        {{ log("Processing Rule: " ~ rule.RULE_SEQ) }}

         {# update GHM columns null if filter is not blank GHM #}



        {#update fresh flag value#}

       {{ log("changed_run_flag values : " ~ fresh_run_flag) }}

         {#end of update null for ghm#}

        {# Construct the update query #}
        {% set update_query %}
            UPDATE {{ rule.TABLENAME }} A
            SET GCGH_REGION = B.GCGH_REGION,
                {% if rule.PATTERN_ID == 'P11' %} {# <underscore>{iso2_ctry_cd}<underscore> #}
                {{ log("Generated SQL:P11") }}
                GCGH_COUNTRY = B.GCGH_COUNTRY,
                COUNTRY_HARMONIZED_BY = '{{ rule.PATTERN_ID }} : {{ rule.COLUMN_NAME }}'
                FROM {{ target.database }}.inditg_integration.vw_harmonization_country_pattern_mapping B
                WHERE A.{{ rule.COLUMN_NAME }} LIKE  '%_' || B.{{ rule.PATTERN_ID }} || '_%'  ESCAPE '_'

                {% elif rule.PATTERN_ID == 'P19' %} {# <underscore>{iso2_ctry_cd}#}
                 {{ log("Generated SQL:P19") }}
                GCGH_COUNTRY = B.GCGH_COUNTRY,
                COUNTRY_HARMONIZED_BY = '{{ rule.PATTERN_ID }} : {{ rule.COLUMN_NAME }}'
                FROM {{ target.database }}.inditg_integration.vw_harmonization_country_pattern_mapping B
                WHERE A.{{ rule.COLUMN_NAME }} LIKE  '%_' || B.{{ rule.PATTERN_ID }} || '%'  ESCAPE '_'

                {% elif rule.PATTERN_ID == 'P20' %} {# <underscore><Space>{iso2_ctry_cd}#}
                {{ log("Generated SQL:P20") }}
                GCGH_COUNTRY = B.GCGH_COUNTRY,
                COUNTRY_HARMONIZED_BY = '{{ rule.PATTERN_ID }} : {{ rule.COLUMN_NAME }}'
                FROM {{ target.database }}.inditg_integration.vw_harmonization_country_pattern_mapping B
                 WHERE A.{{ rule.COLUMN_NAME }} LIKE  '%_ ' || B.{{ rule.PATTERN_ID }} || '%'  ESCAPE '_'

                {% elif rule.PATTERN_ID == 'P21' %} {# {iso2_ctry_cd}<underscore>#}
                {{ log("Generated SQL:P21") }}
                GCGH_COUNTRY = B.GCGH_COUNTRY,
                COUNTRY_HARMONIZED_BY = '{{ rule.PATTERN_ID }} : {{ rule.COLUMN_NAME }}'
                FROM {{ target.database }}.inditg_integration.vw_harmonization_country_pattern_mapping B
                WHERE A.{{ rule.COLUMN_NAME }} LIKE  '%' || B.{{ rule.PATTERN_ID }} || '_%'  ESCAPE '_'

                {% elif rule.PATTERN_ID == 'P25' %} {#{iso2_ctry_cd}<underscore>(AT THE BEGINNING)#}
                {{ log("Generated SQL:P25") }}
                GCGH_COUNTRY = B.GCGH_COUNTRY,
                COUNTRY_HARMONIZED_BY = '{{ rule.PATTERN_ID }} : {{ rule.COLUMN_NAME }}'
                FROM {{ target.database }}.inditg_integration.vw_harmonization_country_pattern_mapping B
                WHERE SUBSTRING(A.{{ rule.COLUMN_NAME }}, 1, 3) LIKE  '%' || B.{{ rule.PATTERN_ID }} || '_%'  ESCAPE '_'

                {% elif rule.PATTERN_ID == 'P59' %} {# {iso3_ctry_cd}<underscore>(AT THE BEGINNING)#}
                {{ log("Generated SQL:P59") }}
                GCGH_COUNTRY = B.GCGH_COUNTRY,
                COUNTRY_HARMONIZED_BY = '{{ rule.PATTERN_ID }} : {{ rule.COLUMN_NAME }}'
                FROM {{ target.database }}.inditg_integration.vw_harmonization_country_pattern_mapping B
                WHERE SUBSTRING(A.{{ rule.COLUMN_NAME }}, 1, 4) LIKE  '%' || B.{{ rule.PATTERN_ID }} || '_%'  ESCAPE '_'

                {% elif rule.PATTERN_ID == 'P26' %} {# <underscore>{iso2_ctry_cd}(AT THE END)#}
                {{ log("Generated SQL:P26") }}
                GCGH_COUNTRY = B.GCGH_COUNTRY,
                COUNTRY_HARMONIZED_BY = '{{ rule.PATTERN_ID }} : {{ rule.COLUMN_NAME }}'
                FROM {{ target.database }}.inditg_integration.vw_harmonization_country_pattern_mapping B
                WHERE RIGHT(A.{{ rule.COLUMN_NAME }}, 3) LIKE  '%' || B.{{ rule.PATTERN_ID }} || '%'

                {% elif rule.PATTERN_ID == 'P33' %} {# <hyphen>{iso2_ctry_cd><underscore>#}
                {{ log("Generated SQL:P33") }}
                GCGH_COUNTRY = B.GCGH_COUNTRY,
                COUNTRY_HARMONIZED_BY = '{{ rule.PATTERN_ID }} : {{ rule.COLUMN_NAME }}'
                FROM {{ target.database }}.inditg_integration.vw_harmonization_country_pattern_mapping B
                WHERE A.{{ rule.COLUMN_NAME }} LIKE  '%-' || B.{{ rule.PATTERN_ID }} || '_%'  ESCAPE '_'

                {% elif rule.PATTERN_ID == 'P34' %} {# {iso2_ctry_cd}<Space><Pipe>(AT THE BEGINNING)#}
                {{ log("Generated SQL:P34") }}
                GCGH_COUNTRY = B.GCGH_COUNTRY,
                COUNTRY_HARMONIZED_BY = '{{ rule.PATTERN_ID }} : {{ rule.COLUMN_NAME }}'
                FROM {{ target.database }}.inditg_integration.vw_harmonization_country_pattern_mapping B
                WHERE SUBSTRING(A.{{ rule.COLUMN_NAME }} , 1, 4) LIKE  '%' || B.{{ rule.PATTERN_ID }} || '%'

                {% elif rule.PATTERN_ID == 'P41' %} {# {iso2_ctry_cd}<hyphen>(AT THE BEGINNING)#}
                {{ log("Generated SQL:P41") }}
                GCGH_COUNTRY = B.GCGH_COUNTRY,
                COUNTRY_HARMONIZED_BY = '{{ rule.PATTERN_ID }} : {{ rule.COLUMN_NAME }}'
                FROM {{ target.database }}.inditg_integration.vw_harmonization_country_pattern_mapping B
                WHERE SUBSTRING(A.{{ rule.COLUMN_NAME }} , 1, 3) LIKE  '%' || B.{{ rule.PATTERN_ID }} || '%'

                {% elif rule.PATTERN_ID == 'P43' %} {# {iso2_ctry_cd}<pipe>(AT THE BEGINNING)#}
                {{ log("Generated SQL:P43") }}
                GCGH_COUNTRY = B.GCGH_COUNTRY,
                COUNTRY_HARMONIZED_BY = '{{ rule.PATTERN_ID }} : {{ rule.COLUMN_NAME }}'
                FROM {{ target.database }}.inditg_integration.vw_harmonization_country_pattern_mapping B
                WHERE SUBSTRING(A.{{ rule.COLUMN_NAME }} , 1, 3) LIKE  '%' || B.{{ rule.PATTERN_ID }} || '%'

                {% elif rule.PATTERN_ID == 'P46' %} {# <Space>{iso2_ctry_cd}(AT THE END)#}
                {{ log("Generated SQL:P46") }}
                GCGH_COUNTRY = B.GCGH_COUNTRY,
                COUNTRY_HARMONIZED_BY = '{{ rule.PATTERN_ID }} : {{ rule.COLUMN_NAME }}'
                FROM {{ target.database }}.inditg_integration.vw_harmonization_country_pattern_mapping B
                WHERE RIGHT(A.{{ rule.COLUMN_NAME }}, 3) LIKE  '%' || B.{{ rule.PATTERN_ID }} || '%'

                {% elif rule.PATTERN_ID == 'P50' %} {# {iso2_ctry_cd}<Space>(AT THE BEGINNING)#}
                {{ log("Generated SQL:P50") }}
                GCGH_COUNTRY = B.GCGH_COUNTRY,
                COUNTRY_HARMONIZED_BY = '{{ rule.PATTERN_ID }} : {{ rule.COLUMN_NAME }}'
                FROM {{ target.database }}.inditg_integration.vw_harmonization_country_pattern_mapping B
                WHERE LEFT(A.{{ rule.COLUMN_NAME }}, 3) LIKE  '%' || B.{{ rule.PATTERN_ID }} || '%'

                {% elif rule.PATTERN_ID == 'P51' %} {# {iso3_ctry_cd}<Space>(AT THE BEGINNING)#}
                {{ log("Generated SQL:P51") }}
                GCGH_COUNTRY = B.GCGH_COUNTRY,
                COUNTRY_HARMONIZED_BY = '{{ rule.PATTERN_ID }} : {{ rule.COLUMN_NAME }}'
                FROM {{ target.database }}.inditg_integration.vw_harmonization_country_pattern_mapping B
                WHERE LEFT(A.{{ rule.COLUMN_NAME }}, 4) LIKE  '%' || B.{{ rule.PATTERN_ID }} || '%'

                {% elif rule.PATTERN_ID == 'P54' %} {#<Space>{iso2_ctry_cd}<underscore>#}
                {{ log("Generated SQL:P54") }}
                GCGH_COUNTRY = B.GCGH_COUNTRY,
                COUNTRY_HARMONIZED_BY = '{{ rule.PATTERN_ID }} : {{ rule.COLUMN_NAME }}'
                FROM {{ target.database }}.inditg_integration.vw_harmonization_country_pattern_mapping B
                WHERE A.{{ rule.COLUMN_NAME }} LIKE  '% ' || B.{{ rule.PATTERN_ID }} || '_%'  ESCAPE '_'

                {% elif rule.PATTERN_ID == 'P55' %} {#<pipe><Space>{iso3_ctry_cd}(AT THE END#}
                {{ log("Generated SQL:P55") }}
                GCGH_COUNTRY = B.GCGH_COUNTRY,
                COUNTRY_HARMONIZED_BY = '{{ rule.PATTERN_ID }} : {{ rule.COLUMN_NAME }}'
                FROM {{ target.database }}.inditg_integration.vw_harmonization_country_pattern_mapping B
                WHERE RIGHT(A.{{ rule.COLUMN_NAME }}, 5) LIKE  '%' || B.{{ rule.PATTERN_ID }} || '%'

                {% elif rule.PATTERN_ID == 'P77' %} {#<hyphen><space>{iso2_ctry_cd}(AT THE END)#}
                {{ log("Generated SQL:P77") }}
                GCGH_COUNTRY = B.GCGH_COUNTRY,
                COUNTRY_HARMONIZED_BY = '{{ rule.PATTERN_ID }} : {{ rule.COLUMN_NAME }}'
                FROM {{ target.database }}.inditg_integration.vw_harmonization_country_pattern_mapping B
                WHERE RIGHT(A.{{ rule.COLUMN_NAME }}, 4) LIKE  '%' || B.{{ rule.PATTERN_ID }} || '%'

                {% elif rule.PATTERN_ID == 'P78' %} {#<underscore>{iso2_ctry_cd}<hyphen>#}
                {{ log("Generated SQL:P78") }}
                GCGH_COUNTRY = B.GCGH_COUNTRY,
                COUNTRY_HARMONIZED_BY = '{{ rule.PATTERN_ID }} : {{ rule.COLUMN_NAME }}'
                FROM {{ target.database }}.inditg_integration.vw_harmonization_country_pattern_mapping B
                WHERE RIGHT(A.{{ rule.COLUMN_NAME }}, 4) LIKE  '%_' || B.{{ rule.PATTERN_ID }} || '-%'  ESCAPE '_'

                {% elif rule.PATTERN_ID == 'EXCEPTION' %}
                {{ log("Generated SQL:EXCEPTION") }}
                    GCGH_COUNTRY = B.MASTER_VALUE,
                    COUNTRY_HARMONIZED_BY ='EXCEPTION : {{ rule.COLUMN_NAME }}'
                    FROM {{ target.database }}.inditg_integration.lkp_harmonization_univeral_exception B
                    WHERE A.{{ rule.COLUMN_NAME }} iLIKE '%' || B.VALUE || '%'
                    AND UPPER(B.HARMONIZATION_TYPE) = 'COUNTRY'
                    AND UPPER(B.exptn_data_src) = UPPER('{{ data_source }}')
                    AND UPPER(B.SOURCE_FIELD) = UPPER('{{ rule.COLUMN_NAME }}')

                {% else %}
                {{ log("Generated SQL:else-Non") }}
                    GCGH_COUNTRY = B.GCGH_COUNTRY,
                    COUNTRY_HARMONIZED_BY = '{{ rule.PATTERN_ID }} : {{ rule.COLUMN_NAME }}'
                    FROM {{ target.database }}.inditg_integration.vw_harmonization_country_pattern_mapping B
                    WHERE A.{{ rule.COLUMN_NAME }} LIKE '%' || B.{{ rule.PATTERN_ID }} || '%'
                {% endif %}
                AND A.GCGH_COUNTRY IS NULL
        {% endset %}

        {#Log the update query#}
        {# Execute the update query #}
        {% if rule.FILTER_CONDITION!='' and rule.FILTER_CONDITION is not none %}
        {% set update_query %}
        {{update_query}} and {{rule.FILTER_CONDITION}}
        {% endset%}
        {#{log("filter not null run Query: " ~ update_query}#}
        {% endif %}

        {#{ log("Update Query to run: " ~ update_query) }#}
        {% do run_query(update_query) %}
    {% endfor %}

    {% set sql_table_null %}
        SELECT
        distinct TABLENAME
        FROM {{ target.database }}.inditg_integration.t_harmonization_rule_mstr
        WHERE
        UPPER(SRC_NAME) = UPPER('{{ data_source }}')
        AND UPPER(HRM_TYPE) = 'BRAND'
        AND ACTIVE_FLAG = 'Y'
    {% endset %}

    {% set update_unharmonized_null = run_query(sql_table_null) %}
    {% for source_table_null in update_unharmonized_null %}
        {{ log("table to update unharmonized:" ~ source_table_null.TABLENAME) }}
                {#update null records to unharmonized#}
        {% set UPDT_NULL_QRY_2 %}
		UPDATE {{ source_table_null.TABLENAME }} A
               SET GCGH_COUNTRY = 'Unharmonized', GCGH_REGION ='Unharmonized',
               COUNTRY_HARMONIZED_BY = 'Unharmonized'
               WHERE GCGH_COUNTRY IS NULL
        {% endset %}
		{% do run_query(UPDT_NULL_QRY_2) %}
    {%endfor%}
    RETURN 'Update completed successfully'
{% endmacro %}
