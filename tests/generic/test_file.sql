{% test test_file(model,compare_columns=None,select_columns=None)%}
{% if compare_columns!=None %}
    select distinct
    {% set file_name_columns = [
                'CDL_SOURCE_FILE', 'FILE_NM', 'SOURCE_FILE_NAME', 'FILENAME', 
                'file_name', 'filename', 'SRC_FILE', 'LOAD_FILE_NM', 'FILE_NAME', 'src_file'
            ] | map('lower') | list %}
            -- Get the actual columns in the model and convert to lowercase
            {% set actual_columns = adapter.get_columns_in_relation(model) | map(attribute='name') | map('lower')|list  %}
            {% set reversed_columns = adapter.get_columns_in_relation(model) | map(attribute='name') | map('lower')|reverse  %}
            -- Loop through file_name_columns to find the first matching column in actual_columns
            {%- for col in reversed_columns %}
                {% if col in file_name_columns%}
                    {{ col }} as file_name
                {% if select_columns or compare_columns %},{% endif %}
                   {% break %}
                {% endif %}   
            {%- endfor %}
        'DISTRIBUTORID IS NOT MATCHING WITH DISTRIBUTOR AVAILABLE IN FILE NAME' AS failure_reason,
                {% if select_columns!=None %}
                    {%- for item in select_columns %}
                        {% if item | lower not in  file_name_columns %}
                            trim({{item}}) as {{item}}
                            {%- if not loop.last -%},{%- endif -%}
                        {%- endif -%}
                    {% endfor %}
                {% else %}
                    {%- for item in compare_columns %}
                        {% if item | lower not in  file_name_columns %}
                            trim({{item}}) as {{item}}
                            {%- if not loop.last -%},{%- endif -%}
                        {%- endif -%}
                    {% endfor %}
                {% endif %}
    from {{model}}
    where
        SUBSTRING(SPLIT_PART(UPPER(TRIM({{compare_columns[0]}})),'_',2),1,3) <> UPPER(TRIM({{compare_columns[1]}}))
        {%- endif -%}
{% endtest %}