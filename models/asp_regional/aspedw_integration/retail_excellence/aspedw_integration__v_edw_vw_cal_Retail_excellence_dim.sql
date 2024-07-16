with EDW_VW_CAL_RETAIL_EXCELLENCE_DIM_trans as
(
SELECT to_char((current_timestamp::character varying)::timestamp without time zone, ('YYYYMM'::varchar)::text) AS curr_mnth,	
to_char(add_months((current_timestamp::varchar)::timestamp without time zone, (- (1)::bigint)),		--//  
('YYYYMM'::character varying)::text) AS prev_mnth, 
to_char(add_months((current_timestamp::varchar)::timestamp without time zone, (- (2)::bigint)),		--//  
('YYYYMM'::character varying)::text) AS last_2mnths, 
to_char(add_months((current_timestamp::varchar)::timestamp without time zone, (- (15)::bigint)),		--//  
('YYYYMM'::character varying)::text) AS last_16mnths,  to_char(add_months((current_timestamp::varchar)::timestamp without time zone, (- (16)::bigint)),		--//  
('YYYYMM'::character varying)::text) AS last_17mnths,  to_char(add_months((current_timestamp::varchar)::timestamp without time zone, (- (26)::bigint)),		--//  
('YYYYMM'::character varying)::text) AS last_27mnths,  to_char(add_months((current_timestamp::varchar)::timestamp without time zone, (- (27)::bigint)),		--//  
('YYYYMM'::character varying)::text) AS last_28mnths

 )
 select * from EDW_VW_CAL_RETAIL_EXCELLENCE_DIM_trans
 

