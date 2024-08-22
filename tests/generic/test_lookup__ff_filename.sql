{% test test_lookup__ff_filename(model,failure_reason="Missing records from master table",column=none,lookup_table=None,lookup_column=None,lookup_filter=None,filters=None,additional_filter=None,extra_col=None)%}
    select 
        '{{failure_reason}}' AS failure_reason,
        *
    from (
        select 
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
                    {{ col }} as file_name,
                   *exclude({{col}})
                   {% break %}
                {% endif %}   
            {%- endfor %}
        {% if extra_col!=None %}
        ,    {{extra_col}}
        {% endif %}
        from {{model}}
        where 
            not {{column}} in (
                select distinct
                    {{lookup_column}}
                from  {{lookup_table}}
                {% if lookup_filter !=None %}
                where {{lookup_filter}}
                
    {% endif %}
    )
    {%- if filters !=None -%}
                and    {{filter}}
    {% endif %}
    ) 
    {%- if additional_filter !=None -%}
    where    {{additional_filter}}
    {% endif %}           
{% endtest %}