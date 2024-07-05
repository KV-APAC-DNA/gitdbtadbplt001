{% macro build_itg_rrl_retailermaster() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    inditg_integration.itg_rrl_retailermaster
                {% else %}
                    {{schema}}.inditg_integration__itg_rrl_retailermaster
                {% endif %}
         (
            retailercode varchar(50),
            retailername varchar(100),
            routecode varchar(25),
            retailerclasscode varchar(50),
            villagecode varchar(50),
            rsdcode varchar(50),
            distributorcode varchar(50),
            foodlicenseno varchar(50),
            druglicenseno varchar(50),
            address varchar(100),
            phone varchar(15),
            mobile varchar(15),
            prcontact varchar(50),
            seccontact varchar(50),
            creditlimit number(18,0),
            creditperiod number(18,0),
            invoicelimit varchar(30),
            isapproved varchar(1),
            isactive boolean,
            rsrcode varchar(100),
            drugvaliditydate timestamp_ntz(9),
            fssaivaliditydate timestamp_ntz(9),
            displaystatus varchar(20),
            createddate timestamp_ntz(9),
            ownername varchar(100),
            druglicenseno2 varchar(50),
            r_statecode number(18,0),
            r_districtcode number(18,0),
            r_tahsilcode number(18,0),
            address1 varchar(100),
            address2 varchar(100),
            retailerchannelcode varchar(40),
            retailerclassid number(18,0),
            actv_flg varchar(1),
            filename varchar(100),
            crt_dttm timestamp_ntz(9),
            updt_dttm timestamp_ntz(9) 
        );

    {% endset %}

    {% do run_query(query) %}
{% endmacro %}