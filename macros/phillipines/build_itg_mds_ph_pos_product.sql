{% macro build_itg_mds_ph_pos_product() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    phlitg_integration.itg_mds_ph_pos_product
                {% else %}
                    {{schema}}.phlitg_integration__itg_mds_ph_pos_product
                {% endif %}
            (
                code varchar(100),
                mnth_id varchar(50),
                item_cd varchar(50),
                bar_cd varchar(50),
                item_nm varchar(255),
                sap_item_cd varchar(50),
                sap_item_desc varchar(255),
                parent_cust_cd varchar(30),
                parent_cust_nm varchar(255),
                jnj_item_desc varchar(255),
                jnj_matl_cse_barcode varchar(50),
                jnj_matl_pc_barcode varchar(50),
                early_bk_period varchar(50),
                cust_conv_factor number(20,4),
                cust_item_prc number(20,4),
                jnj_matl_shipper_barcode varchar(50),
                jnj_matl_consumer_barcode varchar(50),
                jnj_pc_per_cust_unit number(20,4),
                computed_price_per_unit number(20,4),
                jj_price_per_unit number(20,4),
                cust_sku_grp varchar(50),
                uom varchar(50),
                jnj_pc_per_cse number(20,4),
                lst_period varchar(50),
                cust_cd varchar(50),
                cust_cd2 varchar(50),
                last_chg_datetime timestamp_ntz(9),
                effective_from timestamp_ntz(9),
                effective_to timestamp_ntz(9),
                active varchar(10),
                crtd_dttm timestamp_ntz(9),
                updt_dttm timestamp_ntz(9)
        );              
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}



