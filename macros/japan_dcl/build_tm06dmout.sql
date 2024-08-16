{% macro build_tm06dmout() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    jpdcledw_integration.tm06dmout
                {% else %}
                    {{schema}}.jpndcledw_integration__tm06dmout
                {% endif %}
         (
                dmprnno varchar(96) not null,
                dmname varchar(300),
                dmryaku varchar(300),
                outdate number(8,0),
                channelcode varchar(6),
                channelname varchar(60),
                dmdaibucode varchar(6),
                dmdaibunname varchar(300),
                dmchubuncode varchar(9),
                dmchubunname varchar(300),
                dmsyobuncode varchar(12),
                dmsyobunname varchar(300),
                insertdate number(8,0),
                inserttime number(6,0),
                insertid varchar(18),
                updatedate number(8,0),
                updatetime number(6,0),
                updateid varchar(18),
                inserted_date timestamp_ntz(9) default cast('current_timestamp()' as timestamp_ntz(9)),
                inserted_by varchar(100),
                updated_date timestamp_ntz(9) default cast('current_timestamp()' as timestamp_ntz(9)),
                updated_by varchar(100),
                primary key (dmprnno)
            
        );
                               
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}