{% test test_null_filename(model,not_null_columns=None,select_columns=None,filter=none)%}
{% if not_null_columns!=None %}
    select
                    {% set file_name_columns = [
                'CDL_SOURCE_FILE', 'FILE_NM', 'SOURCE_FILE_NAME', 'FILENAME', 
                'file_name', 'filename', 'SRC_FILE', 'LOAD_FILE_NM', 'FILE_NAME', 'src_file'
            ] | map('lower') | list %}


            -- Get the actual columns in the model and convert to lowercase
            {% set actual_columns = adapter.get_columns_in_relation(model) | map(attribute='name') | map('lower')|list  %}
            {% set reversed_columns = adapter.get_columns_in_relation(model) | map(attribute='name') | map('lower')|reverse  %}

           {%- for col in reversed_columns %}
                {% if col in file_name_columns%}
                    {{ col }} as file_name
                {% if select_columns or not_null_columns %},{% endif %}
                   {% break %}
                {% endif %}
                {% if col not in file_name_columns and loop.last %}
                    'Filename N/A' as file_name
                    {% if select_columns or not_null_columns %},{% endif %}
                   {% break %}
                {% endif %}   
            {%- endfor %}
            'KEY COLUMN IS NULL/BLANK' AS failure_reason,
    {% if select_columns!=None %}
        {%- for item in select_columns %}
                {% if item | lower not in  file_name_columns %}
                    trim({{item}}) as {{item}}
                    {%- if not loop.last -%},{%- endif -%}
                {%- endif -%}
        {% endfor %}
    {% else %}
        {%- for item in not_null_columns %}
            {% if item | lower not in  file_name_columns %}
                        trim({{item}}) as {{item}}
                        {%- if not loop.last -%},{%- endif -%}
                    {%- endif -%}
        {% endfor %}
    {% endif %}
    from {{model}}
    where
        {%- for item in not_null_columns %}
        (trim({{item}}) is null or trim({{item}}) = '')
            {%- if not loop.last %} OR
            {%- endif -%}
        {% endfor %}
{%- endif -%}
{%- if filter !=None %}
                and {{filter}} 
            {% endif %}
{% endtest %}