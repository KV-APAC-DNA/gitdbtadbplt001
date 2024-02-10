{% test test_lookup(model,select_columns=None,column=none,lookup_table=None,lookup_column=None,lookup_filter=None,filter=None,additional_filter=None)%}

{% if select_columns!=None %}

    select 
        'KEY COLUMN IS NOT PRESENT IN LOOKUP TABLE' AS failure_reason,
        {%- for item in select_columns %}
        trim({{item}}) as {{item}}
        {%- if not loop.last -%},
        {%- endif -%}
        {% endfor %}

    from (
        select distinct 
            {%- for item in select_columns %}
        trim({{item}}) as {{item}}
        {%- if not loop.last -%},
        {%- endif -%}
        {% endfor %}
        from {{model}}
        where 
            not ({{column}}) in (
                select distinct
                    ({{lookup_column}})
                from  {{lookup_table}}
<<<<<<< HEAD
<<<<<<< HEAD
                {% if lookup_filter !=None %}
=======
                {%- if lookup_filter !=None -%}
>>>>>>> fdd2b73c5777f992a1399e7deb627e00356bfae9
=======
                {%- if lookup_filter !=None -%}
>>>>>>> fdd2b73c5777f992a1399e7deb627e00356bfae9
                where {{lookup_filter}}
                
    {% endif %}
    )
    {%- if filter !=None -%}
                and    {{filter}}
    {% endif %}
    ) 
    {%- if additional_filter !=None -%}
    where    {{additional_filter}}
    {% endif %}          
{% endif %}
    
{% endtest %}