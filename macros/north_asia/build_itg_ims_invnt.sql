{% macro build_itg_ims_invnt() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    ntaitg_integration.itg_ims_invnt
                {% else %}
                    {{schema}}.ntaitg_integration__itg_ims_invnt
                {% endif %}
         (
                invnt_dt date,
                dstr_cd varchar(30),
                dstr_nm varchar(100),
                prod_cd varchar(20),
                prod_nm varchar(200),
                ean_num varchar(20),
                cust_nm varchar(100),
                invnt_qty number(21,5),
                invnt_amt number(21,5),
                avg_prc_amt number(21,5),
                safety_stock number(21,5),
                bad_invnt_qty number(21,5),
                book_invnt_qty number(21,5),
                convs_amt number(21,5),
                prch_disc_amt number(21,5),
                end_invnt_qty number(21,5),
                batch_no varchar(20),
                uom varchar(20),
                sls_rep_cd varchar(20),
                sls_rep_nm varchar(50),
                ctry_cd varchar(2),
                crncy_cd varchar(3),
                crt_dttm timestamp_ntz(9),
                updt_dttm timestamp_ntz(9),
                chn_uom varchar(100),
                storage_name varchar(200)
        );

        
                                
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}



