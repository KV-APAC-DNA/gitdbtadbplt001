{% macro build_insert_no_dup() %}

    {% set query %}
        insert into {% if target.name=='prod' %}
                     jpnwks_integration.wk_so_planet_no_dup
               {% else %}
                     {{schema}}.jpnwks_integration__wk_so_planet_no_dup
               {% endif %}
                    (
                    jcp_rec_seq, id, rcv_dt, test_flag, 
                    bgn_sndr_cd, ws_cd, rtl_type, rtl_cd, 
                    trade_type, shp_date, shp_num, trade_cd, 
                    dep_cd, chg_cd, person_in_charge, 
                    person_name, rtl_name, rtl_ho_cd, 
                    rtl_address_cd, data_type, opt_fld, 
                    item_nm, item_cd_typ, item_cd, qty, 
                    qty_type, price, price_type, bgn_sndr_cd_gln, 
                    rcv_cd_gln, ws_cd_gln, shp_ws_cd, 
                    shp_ws_cd_gln, rep_name_kanji, rep_info, 
                    trade_cd_gln, rtl_cd_gln, rtl_name_kanji, 
                    rtl_ho_cd_gln, item_cd_gtin, item_nm_kanji, 
                    unt_prc, net_prc, sales_chan_type, 
                    jcp_create_date
                    ) 
                    SELECT (SELECT MAX_VALUE::number as max_value FROM {{ ref('jpnedw_integration__mt_constant_seq') }} WHERE 
                    IDENTIFY_CD='SEQUENCE_NO') + ROW_NUMBER() OVER (ORDER BY ID ),
                    id, rcv_dt, test_flag, bgn_sndr_cd, ws_cd, rtl_type, rtl_cd, trade_type, shp_date, shp_num, trade_cd, dep_cd, chg_cd, person_in_charge, person_name, rtl_name, rtl_ho_cd, rtl_address_cd_01, data_type, opt_fld, item_nm, item_cd_typ, item_cd, qty, qty_type, price, price_type, bgn_sndr_cd_gln, rcv_cd_gln, ws_cd_gln, shp_ws_cd, shp_ws_cd_gln, rep_name_kanji, rep_info, trade_cd_gln, rtl_cd_gln, rtl_name_kanji, rtl_ho_cd_gln, item_cd_gtin, item_nm_kanji, unt_prc, net_prc, sales_chan_type, CURRENT_TIMESTAMP()::timestamp_ntz(9)
                    FROM  {{ ref('jpnwks_integration__wk_so_planet_no_dup_temp') }}
                    WHERE jcp_rec_seq IS NULL;

                    UPDATE {{ ref('jpnedw_integration__mt_constant_seq') }}
                    SET MAX_VALUE=(select max(JCP_REC_SEQ) from {{ ref('jpnwks_integration__wk_so_planet_no_dup') }});
      {% endset %}
    {% do run_query(query) %}
{% endmacro %}