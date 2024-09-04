v{% macro build_pcfedw_integration__edw_invoice_fact_snapshot() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    pcfedw_integration.edw_invoice_fact_snapshot
                {% else %}
                    {{schema}}.pcfedw_integration__edw_invoice_fact_snapshot
                {% endif %}
         (
                		snapshot_date date,
                        jj_mnth_id number(18,0),
                        co_cd varchar(4),
                        cust_num varchar(10),
                        matl_num varchar(40),
                        sls_doc varchar(50),
                        curr_key varchar(10),
                        gros_trd_sls number(38,7),
                        cnfrm_qty_pc number(38,7),
                        fut_sls_qty number(38,7)
        );

        
                                
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}



