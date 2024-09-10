{% test test_duplicate_v2(model,group_by_columns=None,select_columns=None,need_counts='yes')%}
{% if group_by_columns!=None %}
    {% set c_pk = "md5(concat(" + group_by_columns|join(",'_',") + "))" %} 
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
            -- Loop through file_name_columns to find the first matching column in actual_columns
            {%- for col in reversed_columns %}
                {% if col in file_name_columns%}
                    {{ col }} as file_name
                {% if not loop.last -%},{% endif %}
                   {% break %}
                {% endif %}   
            {%- endfor %}
               {%- for item in group_by_columns %}
                    {% if item | lower not in  file_name_columns %}
                        coalesce(upper(trim({{item}})),'NA') as {{item}}
                        {%- if not loop.last -%},{%- endif -%}
                    {%- endif -%}
                {%- endfor %}
                {%- if need_counts=='yes' -%},
                count(*) as counts
                {% endif %}
            from {{model}}
            group by 
            file_name,
            {%- for item in group_by_columns %}
                {% if item | lower not in  file_name_columns %}
                    coalesce(upper(trim({{item}})),'NA') 
                    {%- if not loop.last -%},{%- endif -%}
                {%- endif -%}
            {%- endfor %}
            having count(*) >1
        )
        {% if select_columns!=None %}
        select 
                
            'Duplicate records present' AS failure_reason,
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
                   {% break %}
                {% endif %}   
            {%- endfor %}
            {%- for item in group_by_columns %}
                {% if item | lower not in  file_name_columns %}
                    coalesce(upper(trim({{item}})),'NA') as {{item}}
                    {%- if not loop.last -%},{%- endif -%}
                {%- endif -%}
            {%- endfor %}
        from {{model}}
        where (file_name,{{c_pk}}) in (select file_name, {{c_pk}} from grouped_by)
        {% else %}
        select * from grouped_by
        {% endif %}
    
{% endif %}
    
{% endtest %}