{% macro build_edw_ps_msl_items() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    pcfedw_integration.edw_ps_msl_items
                {% else %}
                    {{schema}}.pcfedw_integration__edw_ps_msl_items
                {% endif %}
        (
                		ean varchar(40),
                        retail_environment varchar(100),
                        msl_flag varchar(5),
                        latest_record varchar(5),
                        valid_from date,
                        valid_to date,
                        crt_dttm timestamp_ntz(9),
                        updt_dttm timestamp_ntz(9),
                        msl_rank number(15,0),
                        store_grade_a varchar(5),
                        store_grade_b varchar(5),
                        store_grade_c varchar(5),
                        store_grade_d varchar(5)
        );                               
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}



