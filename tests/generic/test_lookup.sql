{% test test_lookup(model,select_columns=None,column=none,lookup_table=None,lookup_column=None,lookup_filter=None,filter=None,additional_filter=None,failure_reason="'KEY COLUMN IS NOT PRESENT IN LOOKUP TABLE'")%}
{% if select_columns!=None %}
    select 
        {{failure_reason}} as failure_reason,
        file_name,
        {%- for item in select_columns %}
        {% if item | lower not in  file_name_columns %}
            trim({{item}}) as {{item}}
            {%- if not loop.last -%},{%- endif -%}
        {%- endif -%}
        {% endfor %}
    from (
        select distinct 
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
            {%- for item in select_columns %}
                {% if item | lower not in  file_name_columns %}
                                trim({{item}}) as {{item}}
                                {%- if not loop.last -%},{%- endif -%}
                            {%- endif -%}
        {% endfor %}
        from {{model}}
        where 
            ({{column}}) not in (
                select distinct
                    {{lookup_column}}
                from  {{lookup_table}}
                {%- if lookup_filter !=None %}
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