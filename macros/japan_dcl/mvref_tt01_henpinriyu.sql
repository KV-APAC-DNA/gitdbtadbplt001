{% macro mvref_tt01_henpinriyu() %}
    {% set query %}
      UPDATE
		       {{this}} TT01_HENPIN_RIYU
		SET
		     DIHENPINRIYUID = RIYU.DIHENPINRIYUID,
		     updated_date = GETDATE(),
    	     updated_by = 'ETL_Batch'
		FROM
		 (SELECT
		       b.DIINQUIREID,
		       b.DIINQUIREKESAIID,
		       b.DIHENPINRIYUID
		 FROM
		       {{ env_var('DBT_ENV_CORE_DB') }}.JPDCLITG_INTEGRATION.C_TBECINQUIREMEISAI b
		 INNER JOIN 
		 (SELECT
		       MEI.DIINQUIREID as DIINQUIREID,
		       MEI.DIINQUIREKESAIID as DIINQUIREKESAIID,
		       MIN(NVL(MEI.DIMEISAIID,0)) as DIMEISAIID
		 FROM
		       {{ env_var('DBT_ENV_CORE_DB') }}.JPDCLITG_INTEGRATION.C_TBECINQUIREMEISAI MEI
		 INNER JOIN 
		 (SELECT
		       M.DIINQUIREID as DIINQUIREID,
		       M.DIINQUIREKESAIID as DIINQUIREKESAIID,
		       MAX(NVL(M.DITOTALPRC,0)) as maxprc
		 FROM
		       {{ env_var('DBT_ENV_CORE_DB') }}.JPDCLITG_INTEGRATION.C_TBECINQUIREMEISAI M
		 WHERE   
		       M.DIHENPINKUBUN <> 0 AND  
		       M.DSREN >= (Select dateadd(day,(select CAST(ATTR1 as INTEGER) FROM {{ env_var('DBT_ENV_CORE_DB') }}.JPDCLEDW_INTEGRATION.HANYO_ATTR where HANYO_ATTR.KBNMEI = 'DAILYFROM'),CONVERT_TIMEZONE('Asia/Tokyo',current_timestamp())::date))
		 GROUP BY
		       M.DIINQUIREID ,
		       M.DIINQUIREKESAIID ) mtp
		  ON
		       mtp.DIINQUIREID = MEI.DIINQUIREID AND 
		       mtp.maxprc = NVL(MEI.DITOTALPRC,0)
		 WHERE 
		       MEI.DSREN >= (Select dateadd(day,(select CAST(ATTR1 as INTEGER) FROM {{ env_var('DBT_ENV_CORE_DB') }}.JPDCLEDW_INTEGRATION.HANYO_ATTR where HANYO_ATTR.KBNMEI = 'DAILYFROM'),CONVERT_TIMEZONE('Asia/Tokyo',current_timestamp())::date))
		 GROUP BY
		       MEI.DIINQUIREID ,
		       MEI.DIINQUIREKESAIID ) a
		  ON
		       a.DIINQUIREID = b.DIINQUIREID AND 
		       a.DIINQUIREKESAIID = b.DIINQUIREKESAIID AND 
		       a.DIMEISAIID = b.DIMEISAIID) RIYU
		WHERE
		       TT01_HENPIN_RIYU.DIINQUIREID = RIYU.DIINQUIREID AND 
		       TT01_HENPIN_RIYU.DIINQUIREKESAIID = RIYU.DIINQUIREKESAIID
		 ;
    {% endset %}
    {% do run_query(query) %}
{% endmacro %}