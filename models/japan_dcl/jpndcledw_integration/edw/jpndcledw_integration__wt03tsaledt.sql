{{
    config(
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
    )
}}


with tm72del_nm as
(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.TM72DEL_NM
)
,

tm71cancel_nm as
(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.TM71CANCEL_NM
)
,
cim01kokya as
(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIM01KOKYA
)
,
tm40shihanki
as
(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.TM40SHIHANKI
)
,
zaiko_shohin_attr
as
(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.ZAIKO_SHOHIN_ATTR
)
,
tm73nyuhenkan_nm 
as
(
   select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.TM73NYUHENKAN_NM 
)
,
tm67juch_nm
as
(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.TM67JUCH_NM
)
,
tm66torikei_nm
as
(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.TM66TORIKEI_NM
)
,
tm64kessai_nm
as
(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.TM64KESSAI_NM

)
,
tm45cardcorp_nm
as
(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.TM45CARDCORP_NM
)
,
cit81salem
as
(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIT81SALEM
)
,
cit80saleh
as
(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIT80SALEH
)
,
cim43bshobun
as
(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIM43BSHOBUN
)
,
cim42bdaibun
as
(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIM42BDAIBUN
)
,
cim41baitai
as
(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIM41BAITAI
)
,
cim24itbun
as
(
    select * from DEV_DNA_CORE.JPDCLEDW_INTEGRATION.CIM24ITBUN
)
,
cim05opera
as
(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIM05OPERA
)
,
cim03item_zaiko
as
(
    select * from DEV_DNA_CORE.JPDCLEDW_INTEGRATION.CIM03ITEM_ZAIKO
)
,
transformed
as
(
    SELECT cit80.saleno
	,tm40_juch.fy AS juchfy
	,tm40_juch.nendo AS juchnendo
	,tm40_juch.fh AS juchfh
	,tm40_juch.hanki AS juchhanki
	,tm40_juch.qt AS juchqt
	,tm40_juch.shihanki AS juchshihanki
	,CASE 
		WHEN (length(((cit80.juchdate)::CHARACTER VARYING)::TEXT) = 8)
			THEN (
					"substring" (
						((cit80.juchdate)::CHARACTER VARYING)::TEXT
						,1
						,4
						)
					)::CHARACTER VARYING
		ELSE NULL::CHARACTER VARYING
		END AS juchyear
	,CASE 
		WHEN (length(((cit80.juchdate)::CHARACTER VARYING)::TEXT) = 8)
			THEN (
					"substring" (
						((cit80.juchdate)::CHARACTER VARYING)::TEXT
						,5
						,2
						)
					)::CHARACTER VARYING
		ELSE NULL::CHARACTER VARYING
		END AS juchmonth
	,CASE 
		WHEN (length(((cit80.juchdate)::CHARACTER VARYING)::TEXT) = 8)
			THEN cit80.juchdate
		ELSE ((99991231)::NUMERIC)::NUMERIC(18, 0)
		END AS juchdate
	,CASE 
		WHEN (length(((cit80.juchdate)::CHARACTER VARYING)::TEXT) = 8)
			THEN (date_part(DAYOFWEEKISO , dateadd(day,1,to_date(cit80.juchdate :: TEXT ,'YYYYMMDD')) ))
		ELSE NULL::CHARACTER VARYING
		END AS juchyoubicode
	,tm40_shuka.fy AS shukafy
	,tm40_shuka.nendo AS shukanendo
	,tm40_shuka.fh AS shukafh
	,tm40_shuka.hanki AS shukahanki
	,tm40_shuka.qt AS shukaqt
	,tm40_shuka.shihanki AS shukashihanki
	,CASE 
		WHEN (length(((cit80.shukadate)::CHARACTER VARYING)::TEXT) = 8)
			THEN (
					"substring" (
						((cit80.shukadate)::CHARACTER VARYING)::TEXT
						,1
						,4
						)
					)::CHARACTER VARYING
		ELSE NULL::CHARACTER VARYING
		END AS shukayear
	,CASE 
		WHEN (length(((cit80.shukadate)::CHARACTER VARYING)::TEXT) = 8)
			THEN (
					"substring" (
						((cit80.shukadate)::CHARACTER VARYING)::TEXT
						,5
						,2
						)
					)::CHARACTER VARYING
		ELSE NULL::CHARACTER VARYING
		END AS shukamonth
	,CASE 
		WHEN (length(((cit80.shukadate)::CHARACTER VARYING)::TEXT) = 8)
			THEN cit80.shukadate
		ELSE ((99991231)::NUMERIC)::NUMERIC(18, 0)
		END AS shukadate
	,CASE 
		WHEN (length(((cit80.shukadate)::CHARACTER VARYING)::TEXT) = 8)
			THEN (date_part(DAYOFWEEKISO , dateadd(day,1,to_date(cit80.shukadate :: TEXT ,'YYYYMMDD')) ))
		ELSE NULL::CHARACTER VARYING
		END AS shukayoubicode
	,cit80.juchkbn
	,COALESCE(tm67.name, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS juchkbnname
	,COALESCE(tm67.cname, (('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535))::TEXT) AS juchkbncname
	,cit80.cancelflg
	,COALESCE(tm71.name, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS cancelflgname
	,COALESCE(tm71.cname, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS cancelflgcname
	,cit80.torikeikbn
	,COALESCE(tm66.name, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS torikeikbnname
	,COALESCE(tm66.cname, (('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535))::TEXT) AS torikeikbncname
	,cit80.kessaikbn
	,COALESCE(tm64.name, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS kessaikbnname
	,COALESCE(tm64.cname, (('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535))::TEXT) AS kessaikbncname
	,cit80.cardcorpcode
	,COALESCE(tm45.name, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS cardcorpname
	,COALESCE(tm45.cname, (('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535))::TEXT) AS cardcorpcname
	,cit80.nyuhenkanflg
	,COALESCE(tm73.name, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS nyuhenkanflgname
	,COALESCE(tm73.cname, (('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535))::TEXT) AS nyuhenkanflgcname
	,cim01.deleteflg
	,COALESCE(tm72.name, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS deleteflgname
	,COALESCE(tm72.cname, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS deleteflgcname
	,cit80.hanrocode
	,COALESCE(cit80.hanrocode, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS hanrocname
	,COALESCE(cit80.syohanrobunname, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS syohanrobuncname
	,COALESCE(cit80.chuhanrobunname, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS chuhanrobuncname
	,COALESCE(cit80.daihanrobunname, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS daihanrobuncname
	,cit80.mediacode
	,COALESCE(baitai_a.name, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS medianame
	,COALESCE(baitai_a.cname, (('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535))::TEXT) AS mediacname
	,COALESCE(baitai_a.syobuncode, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS mediasyocode
	,COALESCE(baitai_a.syobunname, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS mediasyoname
	,COALESCE(baitai_a.syobuncname, (('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535))::TEXT) AS mediasyocname
	,COALESCE(baitai_a.daibuncode, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS mediadaicode
	,COALESCE(baitai_a.daibunname, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS mediadainame
	,COALESCE(baitai_a.daibuncname, (('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535))::TEXT) AS mediadaicname
	,cim01.firstmediacode
	,COALESCE(baitai_b.name, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS firstmedianame
	,COALESCE(baitai_b.cname, (('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535))::TEXT) AS firstmediacname
	,COALESCE(baitai_b.syobuncode, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS firstmediasyocode
	,COALESCE(baitai_b.syobunname, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS firstmediasyoname
	,COALESCE(baitai_b.syobuncname, (('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535))::TEXT) AS firstmediasyocname
	,COALESCE(baitai_b.daibuncode, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS firstmediadaicode
	,COALESCE(baitai_b.daibunname, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS firstmediadainame
	,COALESCE(baitai_b.daibuncname, (('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535))::TEXT) AS firstmediadaicname
	,cit81.itemcode
	,item.itemname
	,item.itemcname
	,item.chubuncode AS itemchucode
	,item.chubunname AS itemchuname
	,item.chubuncname AS itemchucname
	,item.daibuncode AS itemdaicode
	,item.daibunname AS itemdainame
	,item.daibuncname AS itemdaicname
	,item.daidaibuncode AS itemdaidaicode
	,item.daidaibunname AS itemdaidainame
	,item.daidaibuncname AS itemdaidaicname
	,cim05.logincode AS opecode
	,COALESCE(cim05.opename, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS opename
	,CASE 
		WHEN (
				((cim05.opename)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
				OR (
					(cim05.opename IS NULL)
					-- AND (NULL::'unknown' IS NULL)
					)
				)
			THEN ('その他'::CHARACTER VARYING)::TEXT
		ELSE (((cim05.opecode)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (cim05.opename)::TEXT)
		END AS opecname
	,cit80.kokyano
	,CASE 
		WHEN (length(((cit80.kiboudate)::CHARACTER VARYING)::TEXT) = 8)
			THEN cit80.kiboudate
		ELSE ((99991231)::NUMERIC)::NUMERIC(18, 0)
		END AS kiboudate
	,CASE 
		WHEN (length(((cim01.birthday)::CHARACTER VARYING)::TEXT) = 8)
			THEN cim01.birthday
		ELSE 99991231
		END AS birthday
	,CASE 
		WHEN (length(((cim01.birthday)::CHARACTER VARYING)::TEXT) = 8)
			THEN CASE 
					WHEN (
							(
								(
									(
										"date_part" (
											year
											,current_timestamp()
											)
										)::NUMERIC
									)::NUMERIC(18, 0) - (
									(
										"substring" (
											((cim01.birthday)::CHARACTER VARYING)::TEXT
											,1
											,4
											)
										)::NUMERIC
									)::NUMERIC(18, 0)
								) > ((999)::NUMERIC)::NUMERIC(18, 0)
							)
						THEN ((999)::NUMERIC)::NUMERIC(18, 0)
					ELSE (
							(
								(
									"date_part" (
										year
										,current_timestamp()
										)
									)::NUMERIC
								)::NUMERIC(18, 0) - (
								(
									"substring" (
										((cim01.birthday)::CHARACTER VARYING)::TEXT
										,1
										,4
										)
									)::NUMERIC
								)::NUMERIC(18, 0)
							)
					END
		ELSE ((999)::NUMERIC)::NUMERIC(18, 0)
		END AS nenrei
	,cit81.meisainukikingaku
	,cit81.suryo
	,cit81.meisaitax
	,CASE 
		WHEN (length(((cim01.insertdate)::CHARACTER VARYING)::TEXT) = 8)
			THEN cim01.insertdate
		ELSE 99991231
		END AS insertdate
	,CASE 
		WHEN (length(((cim01.updatedate)::CHARACTER VARYING)::TEXT) = 8)
			THEN cim01.updatedate
		ELSE 99991231
		END AS updatedate
	,cit80.henreasoncode
	,cit80.henreasonname
	,(((cit80.henreasoncode)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (cit80.henreasonname)::TEXT) AS henreasoncname
	,cim01.dmtenpoflg AS dmtenponame
	,cim01.dmtsuhanflg AS dmtsuhanname
	,cim01.kokyakbn AS kokyakbnname
	,cim01.kokyasts AS kokyastsname
	,zattr.bumon1_add_attr1
	,zattr.bumon1_add_attr2
	,zattr.bumon1_add_attr3
	,zattr.bumon1_add_attr4
	,zattr.bumon1_add_attr5
	,zattr.bumon1_add_attr6
	,zattr.bumon1_add_attr7
	,zattr.bumon1_add_attr8
	,zattr.bumon1_add_attr9
	,zattr.bumon1_add_attr10
	,zattr.bumon2_add_attr1
	,zattr.bumon2_add_attr2
	,zattr.bumon2_add_attr3
	,zattr.bumon2_add_attr4
	,zattr.bumon2_add_attr5
	,zattr.bumon2_add_attr6
	,zattr.bumon2_add_attr7
	,zattr.bumon2_add_attr8
	,zattr.bumon2_add_attr9
	,zattr.bumon2_add_attr10
	,zattr.bumon3_add_attr1
	,zattr.bumon3_add_attr2
	,zattr.bumon3_add_attr3
	,zattr.bumon3_add_attr4
	,zattr.bumon3_add_attr5
	,zattr.bumon3_add_attr6
	,zattr.bumon3_add_attr7
	,zattr.bumon3_add_attr8
	,zattr.bumon3_add_attr9
	,zattr.bumon3_add_attr10
	,zattr.bumon4_add_attr1
	,zattr.bumon4_add_attr2
	,zattr.bumon4_add_attr3
	,zattr.bumon4_add_attr4
	,zattr.bumon4_add_attr5
	,zattr.bumon4_add_attr6
	,zattr.bumon4_add_attr7
	,zattr.bumon4_add_attr8
	,zattr.bumon4_add_attr9
	,zattr.bumon4_add_attr10
	,zattr.bumon5_add_attr1
	,zattr.bumon5_add_attr2
	,zattr.bumon5_add_attr3
	,zattr.bumon5_add_attr4
	,zattr.bumon5_add_attr5
	,zattr.bumon5_add_attr6
	,zattr.bumon5_add_attr7
	,zattr.bumon5_add_attr8
	,zattr.bumon5_add_attr9
	,zattr.bumon5_add_attr10
	,zattr.bumon6_add_attr1
	,zattr.bumon6_add_attr2
	,zattr.bumon6_add_attr3
	,zattr.bumon6_add_attr4
	,zattr.bumon6_add_attr5
	,zattr.bumon6_add_attr6
	,zattr.bumon6_add_attr7
	,zattr.bumon6_add_attr8
	,zattr.bumon6_add_attr9
	,zattr.bumon6_add_attr10
	,zattr.bumon6_add_attr11
	,zattr.bumon6_add_attr12
	,zattr.bumon6_add_attr13
	,zattr.bumon6_add_attr14
	,zattr.bumon6_add_attr15
	,zattr.bumon6_add_attr16
	,zattr.bumon6_add_attr17
	,zattr.bumon6_add_attr18
	,zattr.bumon6_add_attr19
	,zattr.bumon6_add_attr20
FROM (
	(
		(
			(
				(
					(
						(
							(
								(
									(
										(
											(
												(
													(
														(
															(
																cit80saleh cit80 JOIN cit81salem cit81 ON (cit80.saleno) = (cit81.saleno)
																) INNER JOIN (
																SELECT cim03.itemcode
																	,COALESCE(cim03.itemname, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS itemname
																	,CASE 
																		WHEN (
																				((cim03.itemname)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
																				OR (
																					(cim03.itemname IS NULL)
																					-- AND (NULL::"unknown" IS NULL)
																					)
																				)
																			THEN ('その他'::CHARACTER VARYING)::TEXT
																		ELSE (((cim03.itemcode)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (cim03.itemname)::TEXT)
																		END AS itemcname
																	,cim03.bunruicode3 AS chubuncode
																	,COALESCE(cim24_chu.itbunname, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS chubunname
																	,CASE 
																		WHEN (
																				((cim24_chu.itbunname)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
																				OR (
																					(cim24_chu.itbunname IS NULL)
																					-- AND (NULL::"unknown" IS NULL)
																					)
																				)
																			THEN ('その他'::CHARACTER VARYING)::TEXT
																		ELSE (((cim03.bunruicode3)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (cim24_chu.itbunname)::TEXT)
																		END AS chubuncname
																	,cim03.bunruicode2 AS daibuncode
																	,COALESCE(cim24_dai.itbunname, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS daibunname
																	,CASE 
																		WHEN (
																				((cim24_dai.itbunname)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
																				OR (
																					(cim24_dai.itbunname IS NULL)
																					-- AND (NULL::"unknown" IS NULL)
																					)
																				)
																			THEN ('その他'::CHARACTER VARYING)::TEXT
																		ELSE (((cim03.bunruicode2)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (cim24_dai.itbunname)::TEXT)
																		END AS daibuncname
																	,cim03.bunruicode5 AS daidaibuncode
																	,COALESCE(cim24_daidai.itbunname, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS daidaibunname
																	,CASE 
																		WHEN (
																				((cim24_daidai.itbunname)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
																				OR (
																					(cim24_daidai.itbunname IS NULL)
																					-- AND (NULL::"unknown" IS NULL)
																					)
																				)
																			THEN ('その他'::CHARACTER VARYING)::TEXT
																		ELSE (((cim03.bunruicode5)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (cim24_daidai.itbunname)::TEXT)
																		END AS daidaibuncname
																FROM (
																	(
																		(
																			cim03item_zaiko cim03 LEFT JOIN cim24itbun cim24_chu ON (
																					(
																						((cim03.bunruicode3)::TEXT = (cim24_chu.itbuncode)::TEXT)
																						AND ((cim24_chu.itbunshcode)::TEXT = ((3)::CHARACTER VARYING)::TEXT)
																						)
																					)
																			) LEFT JOIN cim24itbun cim24_dai ON (
																				(
																					((cim03.bunruicode2)::TEXT = (cim24_dai.itbuncode)::TEXT)
																					AND ((cim24_dai.itbunshcode)::TEXT = ((2)::CHARACTER VARYING)::TEXT)
																					)
																				)
																		) LEFT JOIN cim24itbun cim24_daidai ON (
																			(
																				((cim03.bunruicode5)::TEXT = (cim24_daidai.itbuncode)::TEXT)
																				AND ((cim24_daidai.itbunshcode)::TEXT = ((5)::CHARACTER VARYING)::TEXT)
																				)
																			)
																	)
																) item ON (((cit81.itemcode)::TEXT = (item.itemcode)::TEXT))
															) JOIN cim01kokya cim01 ON (((cit80.kokyano)::TEXT = (cim01.kokyano)::TEXT))
														) LEFT JOIN cim05opera cim05 ON (
															(
																((cit80.insertid)::TEXT = (cim05.opecode)::TEXT)
																AND (
																	(cim05.ciflg)::TEXT = (
																		CASE 
																			WHEN (cit80.marker = ((3)::NUMERIC)::NUMERIC(18, 0))
																				THEN 'PORT'::CHARACTER VARYING
																			ELSE 'NEXT'::CHARACTER VARYING
																			END
																		)::TEXT
																	)
																)
															)
													) LEFT JOIN tm40shihanki tm40_juch ON (
														(
															(cit80.juchdate >= tm40_juch.fromdate)
															AND (cit80.juchdate <= tm40_juch.todate)
															)
														)
												) LEFT JOIN tm40shihanki tm40_shuka ON (
													(
														(cit80.shukadate >= tm40_shuka.fromdate)
														AND (cit80.shukadate <= tm40_shuka.todate)
														)
													)
											) LEFT JOIN tm67juch_nm tm67 ON (((cit80.juchkbn)::TEXT = (tm67.code)::TEXT))
										) LEFT JOIN tm71cancel_nm tm71 ON ((cit80.cancelflg = ((tm71.code)::NUMERIC)::NUMERIC(18, 0)))
									) LEFT JOIN tm66torikei_nm tm66 ON (((cit80.torikeikbn)::TEXT = (tm66.code)::TEXT))
								) LEFT JOIN tm64kessai_nm tm64 ON (((cit80.kessaikbn)::TEXT = (tm64.code)::TEXT))
							) LEFT JOIN tm45cardcorp_nm tm45 ON (((cit80.cardcorpcode)::TEXT = ((tm45.code)::CHARACTER VARYING)::TEXT))
						) LEFT JOIN tm73nyuhenkan_nm tm73 ON ((cit80.nyuhenkanflg = ((tm73.code)::NUMERIC)::NUMERIC(18, 0)))
					) LEFT JOIN tm72del_nm tm72 ON (((cim01.deleteflg)::TEXT = ((tm72.code)::CHARACTER VARYING)::TEXT))
				) LEFT JOIN (
				SELECT cim41.baitaicode AS code
					,COALESCE(cim41.baitainame, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS name
					,CASE 
						WHEN (
								((cim41.baitainame)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
								OR (
									(cim41.baitainame IS NULL)
									-- AND (NULL::"unknown" IS NULL)
									)
								)
							THEN ('その他'::CHARACTER VARYING)::TEXT
						ELSE (((cim41.baitaicode)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (cim41.baitainame)::TEXT)
						END AS cname
					,cim41.sbunruicode AS syobuncode
					,COALESCE(cim43.syobunname, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS syobunname
					,CASE 
						WHEN (
								((cim43.syobunname)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
								OR (
									(cim43.syobunname IS NULL)
									-- AND (NULL::"unknown" IS NULL)
									)
								)
							THEN ('その他'::CHARACTER VARYING)::TEXT
						ELSE (((cim41.sbunruicode)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (cim43.syobunname)::TEXT)
						END AS syobuncname
					,cim43.daibuncode
					,COALESCE(cim42.daibunname, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS daibunname
					,CASE 
						WHEN (
								((cim42.daibunname)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
								OR (
									(cim42.daibunname IS NULL)
									-- AND (NULL::"unknown" IS NULL)
									)
								)
							THEN ('その他'::CHARACTER VARYING)::TEXT
						ELSE (((cim43.daibuncode)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (cim42.daibunname)::TEXT)
						END AS daibuncname
				FROM (
					(
						cim41baitai cim41 JOIN cim43bshobun cim43 ON (((cim41.sbunruicode)::TEXT = (cim43.syobuncode)::TEXT))
						) JOIN cim42bdaibun cim42 ON (((cim43.daibuncode)::TEXT = ((cim42.daibuncode)::CHARACTER VARYING)::TEXT))
					)
				) baitai_a ON (((cit80.mediacode)::TEXT = (baitai_a.code)::TEXT))
			) LEFT JOIN (
			SELECT cim41.baitaicode AS code
				,COALESCE(cim41.baitainame, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS name
				,CASE 
					WHEN (
							((cim41.baitainame)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
							OR (
								(cim41.baitainame IS NULL)
								-- AND (NULL::"unknown" IS NULL)
								)
							)
						THEN ('その他'::CHARACTER VARYING)::TEXT
					ELSE (((cim41.baitaicode)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (cim41.baitainame)::TEXT)
					END AS cname
				,cim41.sbunruicode AS syobuncode
				,COALESCE(cim43.syobunname, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS syobunname
				,CASE 
					WHEN (
							((cim43.syobunname)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
							OR (
								(cim43.syobunname IS NULL)
								-- AND (NULL::"unknown" IS NULL)
								)
							)
						THEN ('その他'::CHARACTER VARYING)::TEXT
					ELSE (((cim41.sbunruicode)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (cim43.syobunname)::TEXT)
					END AS syobuncname
				,cim43.daibuncode
				,COALESCE(cim42.daibunname, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS daibunname
				,CASE 
					WHEN (
							((cim42.daibunname)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
							OR (
								(cim42.daibunname IS NULL)
								-- AND (NULL::"unknown" IS NULL)
								)
							)
						THEN ('その他'::CHARACTER VARYING)::TEXT
					ELSE (((cim43.daibuncode)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (cim42.daibunname)::TEXT)
					END AS daibuncname
			FROM (
				(
					cim41baitai cim41 JOIN cim43bshobun cim43 ON (((cim41.sbunruicode)::TEXT = (cim43.syobuncode)::TEXT))
					) JOIN cim42bdaibun cim42 ON (((cim43.daibuncode)::TEXT = ((cim42.daibuncode)::CHARACTER VARYING)::TEXT))
				)
			) baitai_b ON (((cim01.firstmediacode)::TEXT = (baitai_b.code)::TEXT))
		) LEFT JOIN zaiko_shohin_attr zattr ON (((cit81.itemcode)::TEXT = (zattr.shohin_code)::TEXT))
	)
WHERE ((cit80.torikeikbn)::TEXT = ('01'::CHARACTER VARYING)::TEXT)
)

select * from transformed