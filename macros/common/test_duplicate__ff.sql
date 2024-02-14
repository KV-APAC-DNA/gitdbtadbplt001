{% macro test_duplicate__ff(model,failure_reason=None,group_by_columns=None,count_column=None)%}

{% if group_by_columns!=None %}
    {% set c_pk = "md5(concat(" + group_by_columns|join(",'_',") + "))" %} 
        with grouped_by as(
            select 
                {%- for item in group_by_columns %}
                    {{item}}
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
            group by 
            {% for item in group_by_columns -%}
                {{item}}
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
        select 
             Distinct   
            '{{failure_reason}}' AS failure_reason,
            *
        from {{model}}
        where {{c_pk}} in (select {{c_pk}} from grouped_by)
    
{% endif %}
    
{% endmacro %}