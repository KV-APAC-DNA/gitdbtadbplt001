{% test test_duplicate__ff(model,failure_reason="Duplicate records present",group_by_columns=None,count_column=None,where_condition=None)%}
{% if group_by_columns!=None %}
    {% set file_name_columns = [
                'CDL_SOURCE_FILE', 'FILE_NM', 'SOURCE_FILE_NAME', 'FILENAME', 
                'file_name', 'filename', 'SRC_FILE', 'LOAD_FILE_NM', 'FILE_NAME', 'src_file'
            ] | map('lower') | list %}
            -- Get the actual columns in the model and convert to lowercase
            {% set actual_columns = adapter.get_columns_in_relation(model) | map(attribute='name') | map('lower')|list  %}
            {% set reversed_columns = adapter.get_columns_in_relation(model) | map(attribute='name') | map('lower')|reverse  %}
    {% set group_by_columns_trim = [] %}
    {% for item in group_by_columns %}
        {% if item | lower not in  file_name_columns %}
        {% set group_by_columns_trim_item = "coalesce(upper(trim(" + item + ")), 'NA')" %}
        {% do group_by_columns_trim.append(group_by_columns_trim_item) %}
        {%- endif -%}
    {% endfor %}
    {% set c_pk = "md5(concat(" + group_by_columns_trim|join(",'_',") + "))" %} 
        with grouped_by as(
            select 
            {%- for col in reversed_columns %}
                {% if col in file_name_columns%}
                    {{ col }} as file_name
                {% if  group_by_columns %},{% endif %}
                   {% break %}
                {% endif %}   
            {%- endfor %}
                {%- for item in group_by_columns %}
                        {% if item | lower not in  file_name_columns %}
                             {{item}}
                            {%- if not loop.last and (group_by_columns[loop.index] | lower not in file_name_columns) %}, {% endif %}
                        {%- endif -%}
                    {% endfor %}
            from {{model}}
            {% if where_condition!=None %}
                where {{where_condition}}
            {% endif %}
            group by 
           {%- for col in reversed_columns %}
                {% if col in file_name_columns%}
                    {{ col }}
                {% if  not loop.last %},{% endif %}
                   {% break %}
                {% endif %}   
            {%- endfor %}
            {% for item in group_by_columns -%}
                {% if item | lower not in  file_name_columns %}
                             {{item}}
                            {%- if not loop.last and (group_by_columns[loop.index] | lower not in file_name_columns) %}, {% endif %}
                {%- endif -%}
            {%- endfor %}
            having 
            {% if count_column!=None %}
            count({{count_column}})>1
            {% else %}
            count(*) >1
            {% endif %}
        )
        select 
             Distinct  
            {%- for col in reversed_columns %}
                {% if col in file_name_columns%}
                    {{ col }} as file_name,
                    *exclude({{col}})
                {% if  group_by_columns %},{% endif %}
                   {% break %}
                {% endif %}   
            {%- endfor %} 
            '{{failure_reason}}' AS failure_reason
        from {{model}}
        where ({%- for col in reversed_columns %}
                {% if col in file_name_columns%}
                    file_name
                {% if  group_by_columns %},{% endif %}
                   {% break %}
                {% endif %}   
            {%- endfor %} {{c_pk}}) in (select {%- for col in reversed_columns %}
                {% if col in file_name_columns%}
                     file_name
                {% if  group_by_columns %},{% endif %}
                   {% break %}
                {% endif %}   
            {%- endfor %} {{c_pk}} from grouped_by)
    
{% endif %}
    
{% endtest %}