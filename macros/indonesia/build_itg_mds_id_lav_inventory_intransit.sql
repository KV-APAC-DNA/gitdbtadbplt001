{% macro build_itg_mds_id_lav_inventory_intransit() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    idnitg_integration.itg_mds_id_lav_inventory_intransit
                {% else %}
                    {{schema}}.idnitg_integration__itg_mds_id_lav_inventory_intransit
                {% endif %}
        (
                		month varchar(10),
                        plant varchar(100),
                        saty varchar(100),
                        doc_num varchar(100),
                        po_num varchar(100),
                        matl_num varchar(100),
                        matl_desc varchar(255),
                        ship_to varchar(100),
                        rj_cd varchar(100),
                        rj_reason varchar(255),
                        billing varchar(100),
                        not_invoiced varchar(100),
                        doc_date date,
                        goods_issue date,
                        bill_date date,
                        rdd date,
                        order_qty number(20,4),
                        confirmed_qty number(20,4),
                        net_value number(20,4),
                        billing_check number(20,4),
                        order_value number(20,4),
                        billing_value number(20,4),
                        unrecoverable_ord_val number(20,4),
                        open_orders_val number(20,4),
                        return_value number(20,4),
                        cust_type varchar(100),
                        cust_grp varchar(100),
                        cust_name varchar(255),
                        bill_month varchar(100),
                        return_billing number(20,4),
                        unrecoverable_billing number(20,4),
                        ship_to_2 varchar(100),
                        gi_date date,
                        order_week varchar(100),
                        pod varchar(100),
                        first_day date,
                        remarks varchar(255),
                        name1 varchar(255),
                        created_on1 date,
                        hashkey varchar(255),
                        crtd_dttm timestamp_ntz(9)
        );

        
                                
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}



