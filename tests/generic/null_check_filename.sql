{% test null_check_filename(model, not_null_columns=None, select_columns=None, filter=None) %}
    {% if not_null_columns is not none %}
        select
            'KEY COLUMN IS NULL/BLANK' AS failure_reason,
            -- Define the possible file name columns and convert to lowercase
            {% set file_name_columns = [
                'CDL_SOURCE_FILE', 'FILE_NM', 'SOURCE_FILE_NAME', 'FILENAME', 
                'file_name', 'filename', 'SRC_FILE', 'LOAD_FILE_NM', 'FILE_NAME', 'src_file'
            ] | map('lower') | list %}
            -- Get the actual columns in the model and convert to lowercase
            {% set actual_columns = adapter.get_columns_in_relation(model) | map(attribute='name') | map('lower')|list  %}
            {% set reversed_columns = adapter.get_columns_in_relation(model) | map(attribute='name') | map('lower')|reverse  %}
            -- Log the actual columns and file name columns to debug
            {{ log('Actual Columns: ' ~ actual_columns, info=True) }}
            {{ log('File Name Columns: ' ~ file_name_columns, info=True) }}
            {{ log('File Name Columns_reversed: ' ~ reversed_columns, info=True) }}
            -- Loop through file_name_columns to find the first matching column in actual_columns
            {%- for col in reversed_columns %}
                {% if col in file_name_columns%}
                    {{ col }} as file_name
                {% if select_columns or not_null_columns %},{% endif %}
                   {% break %}
                {% endif %}   
            {%- endfor %}
            -- Include the selected file name column, if foun
            -- Include select_columns if provided, otherwise include not_null_columns
            {% if select_columns is not none %}
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
        
        from {{ model }}
        where
            -- Check for null or blank values in the not_null_columns
            {%- for item in not_null_columns %}
                (trim({{ item }}) is null or trim({{ item }}) = '')
                {%- if not loop.last %} OR {% endif %}
            {% endfor %}
            -- Apply the filter if provided
            {% if filter is not none %}
                and {{ filter }}
            {% endif %}
        {{ log('Completed null_check_filename test', info=True) }}
    {% endif %}
{% endtest %}