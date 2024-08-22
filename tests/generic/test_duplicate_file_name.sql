{% test test_duplicate_file_name(model,group_by_columns=None,select_columns=None,need_counts='yes',count_column=None,filter=None)%}
{% if group_by_columns!=None %}
    {% set group_by_columns_trim = [] %}
    {% for item in group_by_columns %}
        {% set group_by_columns_trim_item = "coalesce(upper(trim(" + item + ")), 'NA')" %}
        {% do group_by_columns_trim.append(group_by_columns_trim_item) %}
    {% endfor %}
    {% set c_pk = "md5(concat(" + group_by_columns_trim|join(",'_',") + "))" %} 
        with grouped_by as(
            select 
                'Duplicate records present' AS failure_reason,
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
                {%- for item in group_by_columns %}
                    {% if item | lower not in  file_name_columns %}
                        coalesce(upper(trim({{item}})),'NA') as {{item}}
                        {%- if not loop.last -%},{%- endif -%}
                    {%- endif -%}
                {%- endfor -%}
                {%- if need_counts=='yes' -%},
                {%- if count_column!=None -%}
                count({{count_column}}) as counts
                {% else %}
                count(*) as counts
                {% endif %}
                {% endif %}
            from {{model}}
            {%- if filter !=None %}
                where {{filter}} 
            {% endif %}
            group by 
            file_name,
            {%- for item in group_by_columns %}
                    {% if item | lower not in  file_name_columns %}
                        coalesce(upper(trim({{item}})),'NA') 
                        {%- if not loop.last -%},{%- endif -%}
                    {%- endif -%}
            {%- endfor -%}
             having 
            {% if count_column!=None %}
            count({{count_column}})>1
            {% else %}
            count(*) >1
            {% endif %}
             
            
            
        )
        {% if select_columns!=None %}
        select 
             Distinct   
            'Duplicate records present' AS failure_reason,
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
            {%- for item in select_columns %}
                    {% if item | lower not in  file_name_columns %}
                        coalesce(upper(trim({{item}})),'NA') as {{item}}
                        {%- if not loop.last -%},{%- endif -%}
                    {%- endif -%}
                {%- endfor -%}
        from {{model}}
        where (file_name,{{c_pk}}) in (select file_name,{{c_pk}} from grouped_by)
        {% else %}
        select * from grouped_by
        {% endif %}
    
{% endif %}
    
{% endtest %}