{% test test_null__ff(model,failure_reason="Null records present",not_null_columns=None,condition="OR")%}
{% if not_null_columns!=None %}
    select
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
                {% if select_columns or not_null_columns %},{% endif %}
                    '{{failure_reason}}' AS failure_reason,
                    *exclude({{col}})
                   {% break %}
                {% endif %}
                {% if col not in file_name_columns and loop.last %}
                    'Filename N/A' as file_name
                    {% if select_columns or not_null_columns %},{% endif %}
                    '{{failure_reason}}' AS failure_reason,
                    *
                   {% break %}
                {% endif %}   
            {%- endfor %}            
    from {{model}}
    where
        {%- for item in not_null_columns %}
        (trim({{item}}) is null or trim({{item}}) = '')
            {%- if not loop.last %} {{condition}}
            {%- endif -%}
        {% endfor %}
{%- endif -%}
{% endtest %}