{% test test_duplicate(model,group_by_columns=None,select_columns=None,need_counts='yes',count_column=None,filter=None)%}


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
                {%- for item in group_by_columns %}
                    coalesce(upper(trim({{item}})),'NA') as {{item}}
                    {%- if not loop.last -%},
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
            {% for item in group_by_columns -%}
                coalesce(upper(trim({{item}})),'NA') 
            {%- if not loop.last -%},
            {% endif %}
            {%- endfor %}
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
            {%- for item in select_columns %}
                coalesce(upper(trim({{item}})),'NA') as {{item}}
                {%- if not loop.last -%},
                {%- endif -%}
            {%- endfor %}
        from {{model}}
        where {{c_pk}} in (select {{c_pk}} from grouped_by)
        {% else %}
        select * from grouped_by
        {% endif %}
    
{% endif %}
    
{% endtest %}