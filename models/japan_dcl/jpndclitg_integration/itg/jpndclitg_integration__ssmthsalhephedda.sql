{{
    config(
        materialized='table',
        pre_hook = ["{{build_ssmthsalhephedda()}}",
        "UPDATE {{this}}
        SET CAN_FLG = 1,
            updated_date = CURRENT_TIMESTAMP(),
            updated_by = 'ETL_Batch'
        WHERE SAL_HEP_NO IN('231700000228','231700001643','231700001644','231700008939','231700008786','231700008787','231700039410','231700039856','231700039857','231700035588','231700040335','231700042887','231700042073','231700041148','231700034437','231700027301','231700046619','231700051303','231700044992') AND KAISHA_CD = 'DCL';"]
    )
}}

with SSMTHSALHEPHEDDA
as
(
    select * from {{source('jpdclitg_integration', 'ssmthsalhephedda')}}     --{{source('jpdclsdl_raw', 'ssmthsalhephedda')}}
),
final as
(
    SELECT KAISHA_CD,
        SAL_HEP_NO,
        SAL_HEP_IMP_SNSH_NO,
        SAL_HEP_JOKYO_KBN,
        PROCESSTYPE_CD,
        KKNG_KBN,
        REND_MT_DEN_SHUBT,
        REND_MT_DEN_NO,
        HASS_MT_DEN_SHUBT,
        HASSEI_MT_DEN_NO,
        SAL_HEP_KEIJ_DT,
        SAL_HEP_DEN_KBN,
        HEP_IC_KBN,
        SAL_HEP_TANTCD,
        SAL_HEP_BMN_NAIBU_NO,
        IKAN_YTI_DT_SAL_HEP,
        IKAN_BMN_NAIBU_NO_SAL_HEP,
        SHOKAI_SAL_HEP_BMN_NAIBU_NO,
        IN_TANTCD,
        PJ_CD,
        RINGISHO_NO,
        TORI_KENMEI,
        SAL_HEP_RIYUU_CD,
        AR_AP_RENKEI_SK_KBN,
        SKCH_TR_SITE_KBN_TOKUISK,
        TOKUISK_CD,
        SHUKASK_CD,
        SKCH_TR_SITE_KBN_SKYSK,
        SKCH_TR_SITE_KBN_SHHARSK,
        SKYSK_CD,
        KAIS_KSSAI_JKEN_NAIBU_NO,
        KAIS_KSSAI_JKEN_NMS,
        KAIS_KSSAI_JKEN_NMR,
        SKYSHIME_KBN,
        SKYSHIME_NAYOSE_KOMK,
        SHHARSK_CD,
        SHHAR_KSSAI_JKEN_NAIBU_NO,
        SHHAR_KSSAI_JKEN_NMS,
        SHHAR_KSSAI_JKEN_NMR,
        SHHARSHIME_KBN,
        SHHARSHIME_NAYOSE_KOMK,
        JUYOUKA_CD,
        SKCH_TR_NAIBU_NO,
        SM_CHANERU_CD,
        KIH_TUKA_CD,
        TATNE_TUKA_CD,
        TATNE_CH_RATE_UNT,
        TATNE_CH_RATE,
        KSAI_TUKA_CD,
        KAJUU_HEIKIN_KSAI_CH_RATE,
        SAL_HEP_SUM_KIN,
        SHOUMI_SAL_HEP_SUM_KIN,
        SAL_HEP_SHOHZEI_SUM_KIN,
        DEN_NEBIKI_RITSU,
        DEN_NEBIKI_KIN,
        SHOUMI_DEN_NEBIKI_KIN,
        DEN_NEBIKI_SZ_KIN,
        DEN_NAF_SAL_HEP_KIN,
        DEN_NF_SHM_SAL_HEP_KIN,
        DEN_NAF_SAL_HEP_SZ_KIN,
        SAL_HEP_SUM_KIN_KSAI,
        SHOUMI_SAL_HEP_SUM_KIN_KSAI,
        SAL_HEP_SZ_SUM_KIN_KSAI,
        DEN_NB_KIN_KSAI,
        SHOUMI_DEN_NB_KIN_KSAI,
        DEN_NB_SZ_KIN_KSAI,
        DEN_NAF_SAL_HEP_KIN_KSAI,
        DEN_NF_SHM_SAL_HEP_KIN_KSAI,
        DEN_NAF_SAL_HEP_SZ_KIN_KSAI,
        SAL_HEP_SUM_KIN_KIH,
        SHOUMI_SAL_HEP_SUM_KIN_KIH,
        SAL_HEP_SZ_SUM_KIN_KIH,
        DEN_NB_KIN_KIH,
        SHOUMI_DEN_NB_KIN_KIH,
        DEN_NB_SZ_KIN_KIH,
        DEN_NAF_SAL_HEP_KIN_KIH,
        DEN_NF_SHM_SAL_HEP_KIN_KIH,
        DEN_NAF_SAL_HEP_SZ_KIN_KIH,
        SAL_GENKA_SUM_KIN,
        SHGK_SUM_KIN,
        SAL_SOU_RIEKI_SUM_KIN,
        SAL_SOU_RIEKI_RITSU,
        HANKAN_HI_TOU_SUM_KIN,
        EIGYOU_RIEKI_SUM_KIN,
        EIGYOU_RIEKI_RITSU,
        NYUKO_SOKO_NAIBU_NO,
        SHKA_YTI_DT,
        NYK_YTI_DT,
        YSOSHDN_CD,
        KSSAI_JKEN_SK_VAL_MOD_FLG,
        SAI_SHKA_FUKUMU_FLG,
        HENKBN_HENKYAK_FUKUMU_FLG,
        HENKBN_HAKI_FUKUMU_FLG,
        HENKBN_BRY_NASHI_FUKUMU_FLG,
        SAL_KJ_JIDO_SHR_FLG,
        SHKA_IR_JIDO_SHR_FLG,
        NYK_IR_JIDO_SHR_FLG,
        SAL_HEP_TEKYO,
        SAL_HEP_BKO_HEP_NOHISHO,
        SAL_HEP_BKO_HEP_SKYSHO,
        SAL_HEP_MEMO,
        SAL_HEP_SAGYOU_IR_NYK,
        SAL_HEP_BKO_NOHISHO,
        SAL_HEP_SAGYOU_IR_SHKA,
        SM_SHNIN_SHNSI_JOKYO_KBN,
        SHOUNIN_SHINSEI_NO,
        ZENKAI_SHOUNIN_SHINSEI_NO,
        HEP_NOHISHO_OUT_JOKYO_KBN,
        HEP_NOS_KEN_SEI_ANAI_OUT_DT,
        SAL_HEP_DATASOSI_JYKY_KBN,
        SAL_HEP_DATA_SOUSHIN_DTTM,
        SAL_HEP_DATA_SSN_CHK_DTTM,
        SAL_HEP_DATA_OUT_JOKYO_KBN,
        SAL_HEP_DATA_OUT_DT,
        SAL_HEP_RIREKI_NO,
        JUCH_NO,
        SNPO_CHUMON_NO,
        SKY_KISAN_DT,
        CAN_FLG,
        CAN_DT,
        CAN_KOUSISHA_CD,
        SM_DEN_ADD_DT,
        DEN_UPD_DT,
        DEN_KOUSISHA,
        TOUROKU_USER,
        TOUROKU_DATE,
        KOUSHIN_USER,
        KOUSHIN_DATE,
        'Src_File_Dt' as SOURCE_FILE_DATE,
        INSERTED_DATE,
        INSERTED_BY,
        UPDATED_DATE,
        UPDATED_BY
    FROM SSMTHSALHEPHEDDA
)
select * from final