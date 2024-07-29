{% macro build_wk_so_planet_cleansed_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                jpnwks_integration.wk_so_planet_cleansed_temp
            {% else %}
                {{schema}}.jpnwks_integration__wk_so_planet_cleansed_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    jpnwks_integration.wk_so_planet_cleansed
                {% else %}
                    {{schema}}.jpnwks_integration__wk_so_planet_cleansed
                {% endif %}
    (       jcp_rec_seq number(10,0),
            id number(10,0),
            rcv_dt date,
            test_flag varchar(256),
            bgn_sndr_cd varchar(256),
            ws_cd varchar(256),
            rtl_type varchar(256),
            rtl_cd varchar(256),
            trade_type varchar(256),
            shp_date date,
            shp_num varchar(256),
            trade_cd varchar(256),
            dep_cd varchar(256),
            chg_cd varchar(256),
            person_in_charge varchar(256),
            person_name varchar(256),
            rtl_name varchar(256),
            rtl_ho_cd varchar(256),
            rtl_address_cd varchar(256),
            data_type varchar(256),
            opt_fld varchar(256),
            item_nm varchar(256),
            item_cd_typ varchar(256),
            item_cd varchar(256),
            qty varchar(256),
            qty_type varchar(256),
            price varchar(256),
            price_type varchar(256),
            bgn_sndr_cd_gln varchar(256),
            rcv_cd_gln varchar(256),
            ws_cd_gln varchar(256),
            shp_ws_cd varchar(256),
            shp_ws_cd_gln varchar(256),
            rep_name_kanji varchar(256),
            rep_info varchar(256),
            trade_cd_gln varchar(256),
            rtl_cd_gln varchar(256),
            rtl_name_kanji varchar(256),
            rtl_ho_cd_gln varchar(256),
            item_cd_gtin varchar(256),
            item_nm_kanji varchar(256),
            unt_prc number(18,0),
            net_prc number(18,0),
            sales_chan_type varchar(256),
            jcp_create_date timestamp_ntz(9),
            jcp_add_qty_c number(18,3),
            jcp_add_c_price number(18,3),
            jcp_add_shp_to_cd varchar(256),
            jcp_add_str_cd varchar(256),
            jcp_add_item_cd_jc varchar(256),
            jcp_mod_qty number(18,3),
            jcp_mod_item_cd varchar(256),
            jcp_mod_price number(18,3),
            jcp_mod_unt_price number(18,3),
            jcp_mod_net_price number(18,3),
            jcp_net_price number(18,3)
    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            jpnwks_integration.wk_so_planet_cleansed
        {% else %}
            {{schema}}.jpnwks_integration__wk_so_planet_cleansed
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}