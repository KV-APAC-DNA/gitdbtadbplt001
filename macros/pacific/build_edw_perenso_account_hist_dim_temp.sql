{% macro build_edw_perenso_account_hist_dim_temp() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    pcfedw_integration.edw_perenso_account_hist_dim
                {% else %}
                    {{schema}}.pcfedw_integration__edw_perenso_account_hist_dim
                {% endif %}
        (
                            acct_id number(10,0),
                            acct_display_name varchar(256),
                            acct_type_desc varchar(50),
                            acct_street_1 varchar(256),
                            acct_street_2 varchar(256),
                            acct_street_3 varchar(256),
                            acct_suburb varchar(25),
                            acct_postcode varchar(25),
                            acct_phone_number varchar(50),
                            acct_fax_number varchar(50),
                            acct_email varchar(256),
                            acct_country varchar(256),
                            acct_region varchar(256),
                            acct_state varchar(256),
                            acct_banner_country varchar(256),
                            acct_banner_division varchar(256),
                            acct_banner_type varchar(256),
                            acct_banner varchar(256),
                            acct_type varchar(256),
                            acct_sub_type varchar(256),
                            acct_grade varchar(256),
                            acct_nz_pharma_country varchar(256),
                            acct_nz_pharma_state varchar(256),
                            acct_nz_pharma_territory varchar(256),
                            acct_nz_groc_country varchar(256),
                            acct_nz_groc_state varchar(256),
                            acct_nz_groc_territory varchar(256),
                            acct_ssr_country varchar(256),
                            acct_ssr_state varchar(256),
                            acct_ssr_team_leader varchar(256),
                            acct_ssr_territory varchar(256),
                            acct_ssr_sub_territory varchar(256),
                            acct_ind_groc_country varchar(256),
                            acct_ind_groc_state varchar(256),
                            acct_ind_groc_territory varchar(256),
                            acct_ind_groc_sub_territory varchar(256),
                            acct_au_pharma_country varchar(256),
                            acct_au_pharma_state varchar(256),
                            acct_au_pharma_territory varchar(256),
                            acct_au_pharma_ssr_country varchar(256),
                            acct_au_pharma_ssr_state varchar(256),
                            acct_au_pharma_ssr_territory varchar(256),
                            acct_store_code varchar(256),
                            acct_fax_opt_out varchar(256),
                            acct_email_opt_out varchar(256),
                            acct_contact_method varchar(256),
                            ssr_grade varchar(256),
                            start_date date,
                            end_date date,
                            hist_flg varchar(5),
                            crt_dttm timestamp_ntz(9),
                            upd_dttm timestamp_ntz(9)

        ); 
        create or replace table {{this}} clone {% if target.name=='prod' %}
                    pcfedw_integration.edw_perenso_account_hist_dim
                {% else %}
                    {{schema}}.pcfedw_integration__edw_perenso_account_hist_dim
                {% endif %};
        UPDATE {{this}} temp
        SET end_date = CURRENT_DATE- 1,
            hist_flg = 'Y'
        FROM (SELECT h.*
            FROM (SELECT *
                    FROM {{this}} 
                    WHERE hist_flg = 'N') h
                LEFT JOIN {{ ref('pcfedw_integration__edw_perenso_account_dim') }} t
                    ON t.acct_id = h.acct_id
                    AND t.acct_store_code = h.acct_store_code
            WHERE (t.acct_store_code IS NULL AND t.acct_id IS NULL)) chng
        WHERE temp.acct_id = chng.acct_id
        AND   temp.acct_store_code = chng.acct_store_code;                             
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}