with tboutcallresult as (
select * from {{ ref('jpndcledw_integration__tboutcallresult') }}
),

tboutcalllatestorder as (
select * from {{ ref('jpndcledw_integration__tboutcalllatestorder') }}
)
,
latest_rt as (
    SELECT diusrid,
            dsroutename
        FROM tboutcalllatestorder
),

final as (
SELECT '0' AS sort_key,
    '顧客ID' AS customer_id,
    '氏' AS last_name,
    '名' AS first_name,
    '氏（カナ）' AS last_name_kana,
    '名（カナ）' AS first_name_kana,
    'TEL（固）' AS tel_home,
    'TEL（携）' AS tel_cell,
    'TEL（他）' AS tel_other,
    'メインTEL' AS tel_main,
    '郵便番号' AS post_code,
    '都道府県' AS prefecture,
    '住所' AS address,
    '初回媒体コード' AS media_code_first,
    '生年月日' AS birthday,
    '職業' AS occupation,
    '最終発送DM' AS final_ship_dm,
    '会員ステージ' AS stage,
    '年齢' AS age,
    '保有ポイント' AS point,
    '仮ポイント' AS point_temp,
    '今月の累計利用ポイント' AS point_curr_mnth,
    '有効期限' AS date_exp,
    '失効ポイント' AS point_exp,
    '受注経路' AS latest_route,
    'カード1(会社名)' AS cred_card_cmp1,
    'カード2(会社名)' AS cred_card_cmp2,
    'カード3(会社名)' AS cred_card_cmp3,
    'カード4(会社名)' AS cred_card_cmp4,
    'カード5(会社名)' AS cred_card_cmp5

UNION ALL

SELECT '1' AS sort_key,
    (((('"'::CHARACTER VARYING)::TEXT || lpad(((outcall_result.diusrid)::CHARACTER VARYING)::TEXT, 10, ((0)::CHARACTER VARYING)::TEXT)) || ('"'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS customer_id,
    (((('"'::CHARACTER VARYING)::TEXT || (outcall_result.dsname)::TEXT) || ('"'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS last_name,
    (((('"'::CHARACTER VARYING)::TEXT || (outcall_result.dsname2)::TEXT) || ('"'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS first_name,
    (((('"'::CHARACTER VARYING)::TEXT || (outcall_result.dskana)::TEXT) || ('"'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS last_name_kana,
    (((('"'::CHARACTER VARYING)::TEXT || (outcall_result.dskana2)::TEXT) || ('"'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS first_name_kana,
    (((('"'::CHARACTER VARYING)::TEXT || (COALESCE(outcall_result.dstel, ''::CHARACTER VARYING))::TEXT) || ('"'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS tel_home,
    (((('"'::CHARACTER VARYING)::TEXT || (COALESCE(outcall_result.dsdat2, ''::CHARACTER VARYING))::TEXT) || ('"'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS tel_cell,
    (((('"'::CHARACTER VARYING)::TEXT || (COALESCE(outcall_result.dsdat3, ''::CHARACTER VARYING))::TEXT) || ('"'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS tel_other,
    (((('"'::CHARACTER VARYING)::TEXT || (COALESCE(outcall_result.dsdat4, ''::CHARACTER VARYING))::TEXT) || ('"'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS tel_main,
    '""' AS post_code,
    (((('"'::CHARACTER VARYING)::TEXT || (COALESCE(outcall_result.dspref, ''::CHARACTER VARYING))::TEXT) || ('"'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS prefecture,
    '""' AS address,
    '""' AS media_code_first,
    '""' AS birthday,
    '""' AS occupation,
    '""' AS final_ship_dm,
    (((('"'::CHARACTER VARYING)::TEXT || (outcall_result.stage)::TEXT) || ('"'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS stage,
    (
        (
            (
                ('"'::CHARACTER VARYING)::TEXT || (
                    (
                        CASE 
                            WHEN (outcall_result.age > 1000)
                                THEN (999)::BIGINT
                            WHEN (outcall_result.age < - 99)
                                THEN (999)::BIGINT
                            ELSE outcall_result.age
                            END
                        )::CHARACTER VARYING
                    )::TEXT
                ) || ('"'::CHARACTER VARYING)::TEXT
            )
        )::CHARACTER VARYING AS age,
    (((('"'::CHARACTER VARYING)::TEXT || ((COALESCE(outcall_result.dipoint, (0)::BIGINT))::CHARACTER VARYING)::TEXT) || ('"'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS point,
    '""' AS point_temp,
    '""' AS point_curr_mnth,
    '""' AS date_exp,
    '""' AS point_exp,
    (((('"'::CHARACTER VARYING)::TEXT || (COALESCE(latest_rt.dsroutename, '直近受注なし'::CHARACTER VARYING))::TEXT) || ('"'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS latest_route,
    '""' AS cred_card_cmp1,
    '""' AS cred_card_cmp2,
    '""' AS cred_card_cmp3,
    '""' AS cred_card_cmp4,
    '""' AS cred_card_cmp5
FROM tboutcallresult outcall_result LEFT JOIN latest_rt ON outcall_result.diusrid = latest_rt.diusrid
WHERE ((outcall_result.excflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
ORDER BY 1,
    2)



select * from final