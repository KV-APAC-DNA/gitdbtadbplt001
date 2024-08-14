with sscmnhin as (
select * from {{ source('jpdclitg_integration', 'sscmnhin') }}
),

hanyo_attr as (
select * from {{ ref('jpndcledw_integration__hanyo_attr') }}
),
syouhincd_henkan as (
select * from {{ ref('jpndcledw_integration__syouhincd_henkan') }}
),

cte1 as (
SELECT hin.hin_cd AS zaiko_itemcode,
    hin.bar_cd2 AS hanbai_itemcode
FROM (
    sscmnhin hin JOIN hanyo_attr hanyo ON (
            (
                (
                    ((hanyo.kbnmei)::TEXT = ('KAISYA'::CHARACTER VARYING)::TEXT)
                    AND ((hin.kaisha_cd)::TEXT = (hanyo.attr1)::TEXT)
                    )
                AND (hin.bar_cd2 IS NOT NULL)
                )
            )
    )
),

cte2 as (
SELECT syouhincd_henkan.koseiocode AS zaiko_itemcode,
    (max((syouhincd_henkan.itemcode)::TEXT))::CHARACTER VARYING AS hanbai_itemcode
FROM syouhincd_henkan
WHERE (
        NOT (
            EXISTS (
                SELECT hin.kaisha_cd,
                    hin.hin_cd,
                    hin.hin_nms,
                    hin.hin_nmr,
                    hin.hin_nmk,
                    hin.hin_nme,
                    hin.hin_katashiki,
                    hin.hin_kbn_cd,
                    hin.hin_grp_cd,
                    hin.hin_bunr_val_cd1,
                    hin.hin_bunr_val_cd2,
                    hin.hin_bunr_val_cd3,
                    hin.hin_bunr_val_cd4,
                    hin.hin_bunr_val_cd5,
                    hin.sm_tori_siyo_flg,
                    hin.pm_tori_siyo_flg,
                    hin.kako_h_siyo_flg,
                    hin.kos_hin_siyo_flg,
                    hin.kikaku_siyo_flg1,
                    hin.kikaku_cd1,
                    hin.kikaku_siyo_flg2,
                    hin.kikaku_cd2,
                    hin.kikaku_siyo_flg3,
                    hin.kikaku_cd3,
                    hin.kikaku_siyo_flg4,
                    hin.kikaku_cd4,
                    hin.kikaku_siyo_flg5,
                    hin.kikaku_cd5,
                    hin.lot_gnbt_kanri_flg1,
                    hin.lot_genka_kanri_flg1,
                    hin.lot_gnbt_kanri_flg2,
                    hin.lot_genka_kanri_flg2,
                    hin.lot_gnbt_kanri_flg3,
                    hin.lot_genka_kanri_flg3,
                    hin.lot_hikat_kanri_flg,
                    hin.lot_hikat_site_kbn,
                    hin.fukusuu_lot_site_flg,
                    hin.lot_hikat_yuko_kigen_spn,
                    hin.lot_yuko_kigen_spn,
                    hin.tairyu_spn,
                    hin.serial_kanri_flg,
                    hin.shohzei_kazei_flg,
                    hin.juch_kanou_spn_start_dt,
                    hin.juch_kanou_spn_end_dt,
                    hin.shka_kanou_spn_start_dt,
                    hin.shka_kanou_spn_end_dt,
                    hin.chotat_kanou_spn_start_dt,
                    hin.chotat_kanou_spn_end_dt,
                    hin.bar_cd1,
                    hin.bar_cd2,
                    hin.bar_cd3,
                    hin.ic_qut_kanri_flg,
                    hin.ic_kin_kanri_flg,
                    hin.unt_ch_hasu_shr_kbn,
                    hin.kih_ic_unt_cd,
                    hin.kih_ic_unt_shosu_siyo_kbn,
                    hin.kih_ic_unt_mass_unt_cd,
                    hin.kih_ic_unt_mass,
                    hin.kih_ic_unt_vol_unt_cd,
                    hin.kih_ic_unt_vol,
                    hin.unt_cd1,
                    hin.unt_cd2,
                    hin.unt_cd3,
                    hin.unt_cd4,
                    hin.unt_cd5,
                    hin.unt_cd6,
                    hin.unt_cd7,
                    hin.unt_cd8,
                    hin.unt_cd9,
                    hin.unt_ch_ritsu1,
                    hin.unt_ch_ritsu2,
                    hin.unt_ch_ritsu3,
                    hin.unt_ch_ritsu4,
                    hin.unt_ch_ritsu5,
                    hin.unt_ch_ritsu6,
                    hin.unt_ch_ritsu7,
                    hin.unt_ch_ritsu8,
                    hin.unt_ch_ritsu9,
                    hin.unt_shosu_siyo_kbn1,
                    hin.unt_shosu_siyo_kbn2,
                    hin.unt_shosu_siyo_kbn3,
                    hin.unt_shosu_siyo_kbn4,
                    hin.unt_shosu_siyo_kbn5,
                    hin.unt_shosu_siyo_kbn6,
                    hin.unt_shosu_siyo_kbn7,
                    hin.unt_shosu_siyo_kbn8,
                    hin.unt_shosu_siyo_kbn9,
                    hin.unt_mass_unt_cd1,
                    hin.unt_mass_unt_cd2,
                    hin.unt_mass_unt_cd3,
                    hin.unt_mass_unt_cd4,
                    hin.unt_mass_unt_cd5,
                    hin.unt_mass_unt_cd6,
                    hin.unt_mass_unt_cd7,
                    hin.unt_mass_unt_cd8,
                    hin.unt_mass_unt_cd9,
                    hin.unt_mass1,
                    hin.unt_mass2,
                    hin.unt_mass3,
                    hin.unt_mass4,
                    hin.unt_mass5,
                    hin.unt_mass6,
                    hin.unt_mass7,
                    hin.unt_mass8,
                    hin.unt_mass9,
                    hin.unt_vol_unt_cd1,
                    hin.unt_vol_unt_cd2,
                    hin.unt_vol_unt_cd3,
                    hin.unt_vol_unt_cd4,
                    hin.unt_vol_unt_cd5,
                    hin.unt_vol_unt_cd6,
                    hin.unt_vol_unt_cd7,
                    hin.unt_vol_unt_cd8,
                    hin.unt_vol_unt_cd9,
                    hin.unt_vol1,
                    hin.unt_vol2,
                    hin.unt_vol3,
                    hin.unt_vol4,
                    hin.unt_vol5,
                    hin.unt_vol6,
                    hin.unt_vol7,
                    hin.unt_vol8,
                    hin.unt_vol9,
                    hin.sal_sz_kbn_krkae_kjun_dt,
                    hin.sal_shohzei_kbn_kirikae_bfo,
                    hin.sal_shohzei_kbn_kirikae_aft,
                    hin.sm_siyo_flg_kih_ic_unt,
                    hin.sm_siyo_flg1,
                    hin.sm_siyo_flg2,
                    hin.sm_siyo_flg3,
                    hin.sm_siyo_flg4,
                    hin.sm_siyo_flg5,
                    hin.sm_siyo_flg6,
                    hin.sm_siyo_flg7,
                    hin.sm_siyo_flg8,
                    hin.sm_siyo_flg9,
                    hin.kih_sm_unt_cd,
                    hin.pm_sz_kbn_krkae_kjun_dt,
                    hin.pm_shohzei_kbn_kirikae_bfo,
                    hin.pm_shohzei_kbn_kirikae_aft,
                    hin.pm_siyo_flg_kih_ic_unt,
                    hin.pm_siyo_flg1,
                    hin.pm_siyo_flg2,
                    hin.pm_siyo_flg3,
                    hin.pm_siyo_flg4,
                    hin.pm_siyo_flg5,
                    hin.pm_siyo_flg6,
                    hin.pm_siyo_flg7,
                    hin.pm_siyo_flg8,
                    hin.pm_siyo_flg9,
                    hin.kih_pm_unt_cd,
                    hin.hyojn_genka,
                    hin.huteikan_kyoyo_flg,
                    hin.huteikan_kyoyo_ritsu_up,
                    hin.huteikan_kyoyo_ritsu_low,
                    hin.kenpldtm,
                    hin.shukaldtm,
                    hin.tytatldtm,
                    hin.sezldtm,
                    hin.kakak_kt_you_hin_bunr_cd,
                    hin.maker_cd,
                    hin.maker_nms,
                    hin.gensan_country_cd,
                    hin.ys_hinmk_kbn,
                    hin.im_hinmk_kbn,
                    hin.hs_cd,
                    hin.anzenhosho_kisei_tais_kbn,
                    hin.tahourei_gaitou_flg,
                    hin.ys_tahourei_nms,
                    hin.im_tahourei_nms,
                    hin.dokb_gekb_kbn,
                    hin.kikenb_kbn,
                    hin.chozo_hoho_kbn,
                    hin.konsai_fuka_flg,
                    hin.haiban_hin_cd,
                    hin.koukei_hin_cd,
                    hin.knp_jisi_flg,
                    hin.ic_hykagae_flg,
                    hin.hin_noki_cal_flg,
                    hin.mrp_tais_hin_flg,
                    hin.hachu_houskkbn,
                    hin.huryo_ritsu,
                    hin.hachu_unt_qut,
                    hin.hachu_unt_spn,
                    hin.safe_ic_qut,
                    hin.chotat_hoho_kbn,
                    hin.futeikan_keij_qut_kbn,
                    hin.saishou_hachu_qut,
                    hin.saidai_hachu_qut,
                    hin.maru_qut,
                    hin.hachutn_qut,
                    hin.hyojn_sm_kakk,
                    hin.nyuko_nyk_tairyu_tukisu,
                    hin.syko_shka_tairyu_tukisu,
                    hin.shnin_shnsi_naibu_no,
                    hin.shiyou_fuka_flg,
                    hin.touroku_user,
                    hin.touroku_date,
                    hin.koushin_user,
                    hin.koushin_date,
                    hanyo.kbnmei,
                    hanyo.attr1,
                    hanyo.attr2,
                    hanyo.attr3,
                    hanyo.attr4,
                    hanyo.attr5,
                    hanyo.insertdate,
                    hanyo.updatedate
                FROM (
                    sscmnhin hin JOIN hanyo_attr hanyo ON (
                            (
                                (
                                    ((hanyo.kbnmei)::TEXT = ('KAISYA'::CHARACTER VARYING)::TEXT)
                                    AND ((hin.kaisha_cd)::TEXT = (hanyo.attr1)::TEXT)
                                    )
                                AND (hin.bar_cd2 IS NOT NULL)
                                )
                            )
                    )
                )
            )
        )
GROUP BY syouhincd_henkan.koseiocode
)
,

final as (
select * from cte1
union all
select * from cte2
)

select * from final