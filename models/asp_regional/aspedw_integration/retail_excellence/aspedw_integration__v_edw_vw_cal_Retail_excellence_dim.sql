with EDW_VW_CAL_RETAIL_EXCELLENCE_DIM_trans as
(
SELECT to_char((current_timestamp::character varying)::timestamp without time zone, ('YYYYMM'::varchar)::text) AS curr_mnth,		
 to_char(add_months((current_timestamp::varchar)::timestamp without time zone, (- (1)::bigint)),		
 ('YYYYMM'::character varying)::text) AS prev_mnth, to_char(add_months((current_timestamp::varchar)::timestamp without time zone,		
 (- (2)::bigint)), ('YYYYMM'::character varying)::text) AS last_2mnths, to_char(add_months((current_timestamp::varchar)::timestamp without time zone,	
 (- (16)::bigint)), ('YYYYMM'::character varying)::text) AS last_16mnths, to_char(add_months((current_timestamp::varchar)::timestamp without time zone,		
 (- (17)::bigint)), ('YYYYMM'::character varying)::text) AS last_17mnths, to_char(add_months((current_timestamp::varchar)::timestamp without time zone,	
 (- (18)::bigint)), ('YYYYMM'::character varying)::text) AS last_18mnths, to_char(add_months((current_timestamp::character varying)::timestamp without time zone, (- (19)::bigint)), ('YYYYMM'::varchar)::text) AS last_19mnths,
to_char(add_months((current_timestamp::character varying)::timestamp without time zone, (- (26)::bigint)), ('YYYYMM'::varchar)::text) AS last_26mnths,
to_char(add_months((current_timestamp::character varying)::timestamp without time zone, (- (27)::bigint)), ('YYYYMM'::varchar)::text) AS last_27mnths,
 to_char(add_months((current_timestamp::character varying)::timestamp without time zone, (- (28)::bigint)), ('YYYYMM'::varchar)::text) AS last_28mnths,
 
 to_char(add_months((current_timestamp::varchar)::timestamp without time zone,		
 (- (36)::bigint)), ('YYYYMM'::varchar)::text) AS last_36mnths,		
 to_char(add_months((current_timestamp::varchar)::timestamp		
 without time zone, (- (37)::bigint)), ('YYYYMM'::varchar)::text) AS last_37mnths
 )
 select * from EDW_VW_CAL_RETAIL_EXCELLENCE_DIM_trans
 

