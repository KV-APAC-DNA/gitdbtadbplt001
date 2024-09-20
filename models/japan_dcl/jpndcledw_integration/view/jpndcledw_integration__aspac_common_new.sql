WITH ssmthsalhephedda
AS (
	SELECT *
	FROM {{ref('jpndclitg_integration__ssmthsalhephedda')}}
	)
	
	,ssmthsalhedda
AS (
	SELECT *
	FROM {{ref('jpndclitg_integration__ssmthsalhedda')}}
	)
	
	,aspac_common_mv
AS (
	SELECT *
	FROM {{ source('jpdcledw_integration', 'aspac_common_mv') }}
	)
	
	,hanyo_partner
AS (
	SELECT *
	FROM {{ref('jpndcledw_integration__hanyo_partner')}}
	)
	
	,cte_join_1
AS (
	SELECT aspacmv.ourino
		,aspacmv.tokuicode
		,aspacmv.tokuiname
		,aspacmv.tokuiname_ryaku
		,aspacmv.tokuiname_kana
		,aspacmv.skysk_name
		,aspacmv.skysk_cd
		,aspacmv.itemkbn
		,aspacmv.itemkbnname
		,aspacmv.senyodenno
		,aspacmv.juchkbn
		,aspacmv.juchkbn_name
		,aspacmv.urikbn
		,aspacmv.juch_bko
		,aspacmv.itemcode
		,aspacmv.jancode
		,aspacmv.tokuiitemcode
		,aspacmv.suryo
		,aspacmv.tanka
		,aspacmv.kingaku
		,aspacmv.shokei
		,aspacmv.tax
		,aspacmv.sogokei
		,aspacmv.shimebi
		,aspacmv.kouritanka
		,(COALESCE(hanyo.attr3, 'その他'::CHARACTER VARYING))::CHARACTER VARYING(60) AS sum_todofuken
		,(COALESCE(hanyo.attr2, 'その他'::CHARACTER VARYING))::CHARACTER VARYING(160) AS tokuiname2
		,aspacmv.todofukennm
		,aspacmv.itemname
		,aspacmv.sensyukai
		,aspacmv.unity_itemname
		,aspacmv.sampleflg
		,(COALESCE(hanyo.attr3, 'その他'::CHARACTER VARYING))::CHARACTER VARYING(60) AS sum_name
		,aspacmv.hansoku_tanka
		,aspacmv.hansoku_ext
		,(COALESCE(hanyo.attr4, 'その他'::CHARACTER VARYING))::CHARACTER VARYING(60) AS sum_tokuiname
		,aspacmv.haibanhinmokucode
		,aspacmv.nohindate
		,aspacmv.shukadate
		,aspacmv.processtype_cd
		,aspacmv.juchno
		,aspacmv.torikeikbn
		,(hanyo.attr5)::CHARACTER VARYING(60) AS tokuizokuseino
		,aspacmv.daihyou_shukask_cd
		,aspacmv.daihyou_shukask_nmr
		,aspacmv.bmn_hyouji_cd
		,aspacmv.bmn_nms
		,CASE 
			WHEN ((aspacmv.juchkbn_name)::TEXT = ('返品（通常）'::CHARACTER VARYING)::TEXT)
				THEN kh.sal_hep_keij_dt
			WHEN ((aspacmv.juchkbn_name)::TEXT = ('通常'::CHARACTER VARYING)::TEXT)
				THEN sh.sal_keij_dt
			ELSE 0
			END AS salekeijodate
		,aspacmv.shohzei_ritsu
	FROM (
		(
			(
				aspac_common_mv aspacmv LEFT JOIN hanyo_partner hanyo ON (
						(
							((hanyo.kbnmei)::TEXT = ('TOKUI'::CHARACTER VARYING)::TEXT)
							AND ((hanyo.attr1)::TEXT = (aspacmv.tokuicode)::TEXT)
							)
						)
				) LEFT JOIN ssmthsalhedda sh ON (((sh.sal_no)::TEXT = (aspacmv.ourino)::TEXT))
			) LEFT JOIN (
			SELECT uhh.kaisha_cd
				,uhh.sal_hep_no
				,uhh.sal_hep_imp_snsh_no
				,uhh.sal_hep_jokyo_kbn
				,uhh.processtype_cd
				,uhh.kkng_kbn
				,uhh.rend_mt_den_shubt
				,uhh.rend_mt_den_no
				,uhh.hass_mt_den_shubt
				,uhh.hassei_mt_den_no
				,uhh.sal_hep_keij_dt
				,uhh.sal_hep_den_kbn
				,uhh.hep_ic_kbn
				,uhh.sal_hep_tantcd
				,uhh.sal_hep_bmn_naibu_no
				,uhh.ikan_yti_dt_sal_hep
				,uhh.ikan_bmn_naibu_no_sal_hep
				,uhh.shokai_sal_hep_bmn_naibu_no
				,uhh.in_tantcd
				,uhh.pj_cd
				,uhh.ringisho_no
				,uhh.tori_kenmei
				,uhh.sal_hep_riyuu_cd
				,uhh.ar_ap_renkei_sk_kbn
				,uhh.skch_tr_site_kbn_tokuisk
				,uhh.tokuisk_cd
				,uhh.shukask_cd
				,uhh.skch_tr_site_kbn_skysk
				,uhh.skch_tr_site_kbn_shharsk
				,uhh.skysk_cd
				,uhh.kais_kssai_jken_naibu_no
				,uhh.kais_kssai_jken_nms
				,uhh.kais_kssai_jken_nmr
				,uhh.skyshime_kbn
				,uhh.skyshime_nayose_komk
				,uhh.shharsk_cd
				,uhh.shhar_kssai_jken_naibu_no
				,uhh.shhar_kssai_jken_nms
				,uhh.shhar_kssai_jken_nmr
				,uhh.shharshime_kbn
				,uhh.shharshime_nayose_komk
				,uhh.juyouka_cd
				,uhh.skch_tr_naibu_no
				,uhh.sm_chaneru_cd
				,uhh.kih_tuka_cd
				,uhh.tatne_tuka_cd
				,uhh.tatne_ch_rate_unt
				,uhh.tatne_ch_rate
				,uhh.ksai_tuka_cd
				,uhh.kajuu_heikin_ksai_ch_rate
				,uhh.sal_hep_sum_kin
				,uhh.shoumi_sal_hep_sum_kin
				,uhh.sal_hep_shohzei_sum_kin
				,uhh.den_nebiki_ritsu
				,uhh.den_nebiki_kin
				,uhh.shoumi_den_nebiki_kin
				,uhh.den_nebiki_sz_kin
				,uhh.den_naf_sal_hep_kin
				,uhh.den_nf_shm_sal_hep_kin
				,uhh.den_naf_sal_hep_sz_kin
				,uhh.sal_hep_sum_kin_ksai
				,uhh.shoumi_sal_hep_sum_kin_ksai
				,uhh.sal_hep_sz_sum_kin_ksai
				,uhh.den_nb_kin_ksai
				,uhh.shoumi_den_nb_kin_ksai
				,uhh.den_nb_sz_kin_ksai
				,uhh.den_naf_sal_hep_kin_ksai
				,uhh.den_nf_shm_sal_hep_kin_ksai
				,uhh.den_naf_sal_hep_sz_kin_ksai
				,uhh.sal_hep_sum_kin_kih
				,uhh.shoumi_sal_hep_sum_kin_kih
				,uhh.sal_hep_sz_sum_kin_kih
				,uhh.den_nb_kin_kih
				,uhh.shoumi_den_nb_kin_kih
				,uhh.den_nb_sz_kin_kih
				,uhh.den_naf_sal_hep_kin_kih
				,uhh.den_nf_shm_sal_hep_kin_kih
				,uhh.den_naf_sal_hep_sz_kin_kih
				,uhh.sal_genka_sum_kin
				,uhh.shgk_sum_kin
				,uhh.sal_sou_rieki_sum_kin
				,uhh.sal_sou_rieki_ritsu
				,uhh.hankan_hi_tou_sum_kin
				,uhh.eigyou_rieki_sum_kin
				,uhh.eigyou_rieki_ritsu
				,uhh.nyuko_soko_naibu_no
				,uhh.shka_yti_dt
				,uhh.nyk_yti_dt
				,uhh.ysoshdn_cd
				,uhh.kssai_jken_sk_val_mod_flg
				,uhh.sai_shka_fukumu_flg
				,uhh.henkbn_henkyak_fukumu_flg
				,uhh.henkbn_haki_fukumu_flg
				,uhh.henkbn_bry_nashi_fukumu_flg
				,uhh.sal_kj_jido_shr_flg
				,uhh.shka_ir_jido_shr_flg
				,uhh.nyk_ir_jido_shr_flg
				,uhh.sal_hep_tekyo
				,uhh.sal_hep_bko_hep_nohisho
				,uhh.sal_hep_bko_hep_skysho
				,uhh.sal_hep_memo
				,uhh.sal_hep_sagyou_ir_nyk
				,uhh.sal_hep_bko_nohisho
				,uhh.sal_hep_sagyou_ir_shka
				,uhh.sm_shnin_shnsi_jokyo_kbn
				,uhh.shounin_shinsei_no
				,uhh.zenkai_shounin_shinsei_no
				,uhh.hep_nohisho_out_jokyo_kbn
				,uhh.hep_nos_ken_sei_anai_out_dt
				,uhh.sal_hep_datasosi_jyky_kbn
				,uhh.sal_hep_data_soushin_dttm
				,uhh.sal_hep_data_ssn_chk_dttm
				,uhh.sal_hep_data_out_jokyo_kbn
				,uhh.sal_hep_data_out_dt
				,uhh.sal_hep_rireki_no
				,uhh.juch_no
				,uhh.snpo_chumon_no
				,uhh.sky_kisan_dt
				,uhh.can_flg
				,uhh.can_dt
				,uhh.can_kousisha_cd
				,uhh.sm_den_add_dt
				,uhh.den_upd_dt
				,uhh.den_kousisha
				,uhh.touroku_user
				,uhh.touroku_date
				,uhh.koushin_user
				,uhh.koushin_date
				,uhh.source_file_date
				,uhh.inserted_date
				,uhh.inserted_by
				,uhh.updated_date
				,uhh.updated_by
				,uh.kaisha_cd
				,uh.sal_no
				,uh.sal_jisk_imp_snsh_no
				,uh.sal_shur_kbn
				,uh.sal_den_kbn
				,uh.shanigi_kbn
				,uh.kkng_kbn
				,uh.den_add_kbn
				,uh.skch_tr_site_kbn
				,uh.tori_kbn
				,uh.tori_kenmei
				,uh.sm_chaneru_cd
				,uh.musyo_juch_kbn
				,uh.musyo_tori_riyuu_cd
				,uh.rend_mt_den_shubt
				,uh.rend_mt_den_no
				,uh.hass_mt_den_shubt
				,uh.hassei_mt_den_no
				,uh.shanai_tori_hachu_no
				,uh.juhachu_tori_pm_no
				,uh.sal_keij_dt
				,uh.sal_tantcd
				,uh.sal_bmn_naibu_no
				,uh.in_tantcd
				,uh.tokuisk_cd
				,uh.snpo_chumon_no
				,uh.tokuisk_snpo_tant_nms
				,uh.tokuisk_snpo_tant_tel
				,uh.tokuisk_snpo_tant_fax
				,uh.shharsk_cd
				,uh.shhar_kssai_jken_naibu_no
				,uh.shhar_kssai_jken_nms
				,uh.shhar_kssai_jken_nmr
				,uh.shharshime_kbn
				,uh.shharshime_nayose_komk
				,uh.skysk_cd
				,uh.kais_kssai_jken_naibu_no
				,uh.kais_kssai_jken_nms
				,uh.kais_kssai_jken_nmr
				,uh.skyshime_kbn
				,uh.skyshime_nayose_komk
				,uh.sm_skysk_yuubin_no
				,uh.sm_skysk_addr1
				,uh.sm_skysk_addr2
				,uh.sm_skysk_tant_nms
				,uh.sm_skysk_tel
				,uh.sm_skysk_fax
				,uh.shukask_cd
				,uh.shukask_nms1
				,uh.shukask_nms2
				,uh.shukask_yuubin_no
				,uh.shukask_addr1
				,uh.shukask_addr2
				,uh.shukask_tant_nms
				,uh.shukask_tel
				,uh.shukask_fax
				,uh.juyouka_cd
				,uh.skch_tr_naibu_no
				,uh.soko_naibu_no
				,uh.pj_cd
				,uh.tatne_tuka_cd
				,uh.tatne_ch_rate_unt
				,uh.tatne_ch_rate
				,uh.ksai_tuka_cd
				,uh.ksai_ch_rate_unt
				,uh.kajuu_heikin_ksai_ch_rate
				,uh.sal_sum_kin
				,uh.shoumi_sal_sum_kin
				,uh.sal_shohzei_sum_kin
				,uh.den_nebiki_ritsu
				,uh.den_nebiki_kin
				,uh.shoumi_den_nebiki_kin
				,uh.den_nebiki_sz_kin
				,uh.den_naf_sal_kin
				,uh.den_nf_shm_sal_kin
				,uh.den_naf_sal_sz_kin
				,uh.sal_sum_kin_ksai
				,uh.shoumi_sal_sum_kin_ksai
				,uh.sal_sz_sum_kin_ksai
				,uh.den_nb_kin_ksai
				,uh.shoumi_den_nb_kin_ksai
				,uh.den_nb_sz_kin_ksai
				,uh.den_naf_sal_kin_ksai
				,uh.den_nf_shm_sal_kin_ksai
				,uh.den_naf_sal_sz_kin_ksai
				,uh.sal_sum_kin_kih
				,uh.shoumi_sal_sum_kin_kih
				,uh.sal_sz_sum_kin_kih
				,uh.den_nb_kin_kih
				,uh.shoumi_den_nb_kin_kih
				,uh.den_nb_sz_kin_kih
				,uh.den_naf_sal_kin_kih
				,uh.den_nf_shm_sal_kin_kih
				,uh.den_naf_sal_sz_kin_kih
				,uh.sal_genka_sum_kin
				,uh.shgk_sum_kin
				,uh.sal_sou_rieki_sum_kin
				,uh.sal_sou_rieki_ritsu
				,uh.hankan_hi_tou_sum_kin
				,uh.eigyou_rieki_sum_kin
				,uh.eigyou_rieki_ritsu
				,uh.shokai_sal_dt
				,uh.upd_mt_sal_no
				,uh.upd_sal_no
				,uh.sosai_sal_no
				,uh.sal_tekyo
				,uh.sal_bko
				,uh.sal_memo
				,uh.kakak_kt_key_cd
				,uh.shokai_sal_no
				,uh.kssai_jken_mod_flg
				,uh.den_naf_sal_kin_mod_flg
				,uh.sal_bnktsu_flg
				,uh.yoshin_gendo_kin_over_flg
				,uh.skch_tr_gndgk_over_flg
				,uh.musyo_tori_fukumu_flg
				,uh.teiki_sal_fukumu_flg
				,uh.meu_sky_flg
				,uh.sm_shnin_shnsi_jokyo_kbn
				,uh.shounin_shinsei_no
				,uh.zenkai_shounin_shinsei_no
				,uh.sm_shnin_shnsi_jokyo_kbn_dn
				,uh.shnin_shnsi_no_den
				,uh.ringisho_no
				,uh.siwk_add_jokyo_kbn
				,uh.can_siwk_add_jokyo_kbn
				,uh.siwk_renkei_naibu_no
				,uh.jidou_siwk_run_seq_no
				,uh.ap_renkei_jokyo_kbn
				,uh.ar_renkei_jokyo_kbn
				,uh.ar_ap_renkei_sk_kbn
				,uh.sal_rireki_no
				,uh.sky_den_naibu_no
				,uh.shhar_den_naibu_no
				,uh.sm_siwk_naibu_no
				,uh.processtype_cd
				,uh.jyu_hachu_kbn
				,uh.sougk_jungk_kbn
				,uh.kwse_yoyaku_kanri_kbn
				,uh.kwse_yoyaku_kaikei_shr_kbn
				,uh.tokuisk_shus_joho_hyoji_flg
				,uh.skysk_shus_joho_hyoji_flg
				,uh.shukask_shus_joho_hyoji_flg
				,uh.can_flg
				,uh.can_dt
				,uh.can_kousisha_cd
				,uh.sm_den_add_dt
				,uh.den_upd_dt
				,uh.den_kousisha
				,uh.touroku_user
				,uh.touroku_date
				,uh.koushin_user
				,uh.koushin_date
				,uh.source_file_date
				,uh.inserted_date
				,uh.inserted_by
				,uh.updated_date
				,uh.updated_by
			FROM (
				ssmthsalhephedda uhh JOIN ssmthsalhedda uh ON (((uh.rend_mt_den_no)::TEXT = (uhh.sal_hep_no)::TEXT))
				)
			) kh ON (((kh.sal_no)::TEXT = (aspacmv.ourino)::TEXT))
		)
	)
	
SELECT *
FROM cte_join_1
