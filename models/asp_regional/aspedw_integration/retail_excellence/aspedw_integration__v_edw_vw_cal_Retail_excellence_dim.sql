with edw_vw_cal_retail_excellence_dim as (
    select * from {{ source('aspedw_integration', 'edw_vw_cal_retail_excellence_dim') }}
),
EDW_VW_CAL_RETAIL_EXCELLENCE_DIM_trans as
(
SELECT to_char((current_timestamp::character varying)::timestamp without time zone, ('YYYYMM'::varchar)::text) AS curr_mnth,		--// SELECT to_char(('now'::character varying)::timestamp without time zone, ('YYYYMM'::character varying)::text) AS curr_mnth, // character varying
 to_char(add_months((current_timestamp::varchar)::timestamp without time zone, (- (1)::bigint)),		--//  to_char(add_months(('now'::character varying)::timestamp without time zone, (- (1)::bigint)), // character varying
 ('YYYYMM'::character varying)::text) AS prev_mnth, to_char(add_months((current_timestamp::varchar)::timestamp without time zone,		--//  ('YYYYMM'::character varying)::text) AS prev_mnth, to_char(add_months(('now'::character varying)::timestamp without time zone, // character varying
 (- (2)::bigint)), ('YYYYMM'::character varying)::text) AS last_2mnths, to_char(add_months((current_timestamp::varchar)::timestamp without time zone,		--//  (- (2)::bigint)), ('YYYYMM'::character varying)::text) AS last_2mnths, to_char(add_months(('now'::character varying)::timestamp without time zone, // character varying
 (- (18)::bigint)), ('YYYYMM'::character varying)::text) AS last_18mnths, to_char(add_months((current_timestamp::character varying)::timestamp without time zone, (- (19)::bigint)), ('YYYYMM'::varchar)::text) AS last_19mnths,
to_char(add_months((current_timestamp::character varying)::timestamp without time zone, (- (26)::bigint)), ('YYYYMM'::varchar)::text) AS last_26mnths,
to_char(add_months((current_timestamp::character varying)::timestamp without time zone, (- (27)::bigint)), ('YYYYMM'::varchar)::text) AS last_27mnths,
 --//  (- (18)::bigint)), ('YYYYMM'::character varying)::text) AS last_18mnths, to_char(add_months(('now'::character varying)::timestamp without time zone, (- (19)::bigint)), ('YYYYMM'::character varying)::text) AS last_19mnths, // character varying
 to_char(add_months((current_timestamp::varchar)::timestamp without time zone,		--//  to_char(add_months(('now'::character varying)::timestamp without time zone, // character varying
 (- (36)::bigint)), ('YYYYMM'::varchar)::text) AS last_36mnths,		--// character varying
 to_char(add_months((current_timestamp::varchar)::timestamp		--//  to_char(add_months(('now'::character varying)::timestamp // character varying
 without time zone, (- (37)::bigint)), ('YYYYMM'::varchar)::text) AS last_37mnths
 )
 select * from EDW_VW_CAL_RETAIL_EXCELLENCE_DIM_trans
 

