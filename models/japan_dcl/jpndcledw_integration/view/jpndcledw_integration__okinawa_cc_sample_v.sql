with tbusrpram as (
    select * from {{ ref('jpndclitg_integration__tbusrpram') }}
),

cte1 as (
SELECT '0' AS sort_key,
    'TEL（固）' AS tel_home,
    'TEL（携）' AS tel_cell,
    'TEL（他）' AS tel_other,
    'メインフラグ' AS tel_main,
    '氏' AS last_name,
    '名' AS first_name,
    '氏（カナ）' AS last_name_kana,
    '名（カナ）' AS first_name_kana,
    '郵便番号' AS post_code,
    '都道府県' AS prefecture,
    '住所' AS address
),

cte2 as (
SELECT '1' AS sort_key,
    tbusrpram.dstel AS tel_home,
    tbusrpram.dsdat2 AS tel_cell,
    tbusrpram.dsdat3 AS tel_other,
    tbusrpram.dsdat4 AS tel_main,
    tbusrpram.dsname AS last_name,
    tbusrpram.dsname2 AS first_name,
    tbusrpram.dskana AS last_name_kana,
    tbusrpram.dskana2 AS first_name_kana,
    tbusrpram.dszip AS post_code,
    tbusrpram.dspref AS prefecture,
    ((((COALESCE(tbusrpram.dscity, ''::CHARACTER VARYING))::TEXT || (COALESCE(tbusrpram.dsaddr, ''::CHARACTER VARYING))::TEXT) || (COALESCE(tbusrpram.dstatemono, ''::CHARACTER VARYING))::TEXT))::CHARACTER VARYING AS address
FROM tbusrpram
WHERE (
        (
            (
                ((tbusrpram.dsdat12)::TEXT = 'サンプル魔'::TEXT)
                OR ((tbusrpram.dsdat12)::TEXT = '悪質サンプル魔'::TEXT)
                )
            AND ((tbusrpram.disecessionflg)::TEXT = '0'::TEXT)
            )
        AND ((tbusrpram.dielimflg)::TEXT = '0'::TEXT)
        )
),

final as (
    select * from cte1
    union all
    select * from cte2
)

select * from final