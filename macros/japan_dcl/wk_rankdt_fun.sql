{% macro wk_rankdt_fun() %}

    {% if target.name=='prod' %}
        {% set source_table %}
                        JPDCLEDW_INTEGRATION.KR_COMM_POINT_PARA
                    {% endset %}
        {% set target_table %}
                        JPDCLEDW_INTEGRATION.WK_RANKDT_TMP
                    {% endset %}

    {% else %}
        {% set source_table %}
                        JPDCLEDW_INTEGRATION__KR_COMM_POINT_PARA
                    {% endset %}
        {% set target_table %}
                        JPDCLEDW_INTEGRATION__WK_RANKDT_TMP_demo
                    {% endset %}
    {% endif %}


    {% set month1 = 1 %}

    {% set year_query %} 
        select extract(year from convert_timezone('UTC','Asia/Tokyo',current_timestamp()));
    {% endset %}

    {% set target_year_query %} 
        select target_year from {{ source_table }};
    {% endset %}

    {% set term_end_query %} 
        select substring(term_end,5,2) from {{ source_table }};
    {% endset %}

    {% if execute %}
        {% set year = run_query(year_query) %}
        {% set year1 = year.columns[0][0] %}


        {% set target_year = run_query(target_year_query) %}
        {% set target_year1 = target_year.columns[0][0]|string %}

        {% set term_end = run_query(term_end_query) %}
        {% set term_end1 = term_end.columns[0][0] %}




        {% if cast(year1 , integer) >  cast(target_year1 , integer) %}
            {% set end_month = 12 %}
        {% else %}
            {% set end_month = term_end1|int %}
        {% endif %}
            

        

            {% set query %}
                insert into {{ target_table }} (rankdt,price)
                {% for i in range(1,end_month + 1) %}
                select {{ target_year1 }} || lpad({{ i }}::varchar,2,'00') , 0
                {% if not loop.last %}
                    union all
                {% else %}
                    ;
                {% endif %}
                

                {% endfor %}
            {% endset %}

            {% do run_query(query) %}

        

    {% endif %}
    {{ log('done inserting', info=True) }}
    

{% endmacro %}
