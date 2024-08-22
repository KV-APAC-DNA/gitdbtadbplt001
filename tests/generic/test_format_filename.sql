{% test test_format_filename(model,select_columns=None,failure_reason="'Invalid Records'",where_clause=None)%}
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
                {% if not loop.last  %},{% endif %}
                   {% break %}
                {% endif %} 
            {%- endfor %}
                {{failure_reason}} AS failure_reason,
                {% if select_columns!=None %}    
                    {%- for item in select_columns %}
                        {% if item | lower not in  file_name_columns %}
                        trim({{item}}) as {{item}}
                        {%- if not loop.last -%},{%- endif -%}
                    {%- endif -%}
                    {% endfor %}
                {% else %}
                        *exclude{{col}}
                {% endif %}
    from {{model}}
    {% if where_clause!=None %}
    where
        {{where_clause}}
    {% endif %}
{% endtest %}