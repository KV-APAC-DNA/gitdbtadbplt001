with list_teiki_remind_new2_v as (
    select * from {{ ref('jpndcledw_integration__list_teiki_remind_new2_v') }}
),
final as (
    SELECT email,
        webid,
        last_name,
        first_name,
        otodoke_day,
        otodoke_time,
        listagg((items)::text, ' ・ '::text) WITHIN GROUP(
            ORDER BY sort_key,
                usrid,
                email,
                webid,
                items
        ) AS items,
        payment,
        otodoke_place,
        status,
        henkoulimit,
        adrs1 AS otdk_zip,
        adrs2 AS otdk_pref,
        adrs3 AS otdk_city,
        replace(
            replace((adrs4)::text, '−'::text, 'ー'::text),
            '－'::text,
            'ー'::text
        ) AS otdk_addr,
        replace(
            replace((adrs5)::text, '−'::text, 'ー'::text),
            '－'::text,
            'ー'::text
        ) AS otdk_tatemono,
        otodokesei AS otdk_sei,
        otodokemei AS otdk_mei,
        exec_time AS exec_date,
        CASE
            WHEN (sort_key = 1) THEN (({{encryption_1('usrid')}})::bigint)::character varying
            ELSE usrid
        END AS userid
    FROM list_teiki_remind_new2_v
    GROUP BY email,
        webid,
        last_name,
        first_name,
        otodoke_day,
        otodoke_time,
        payment,
        otodoke_place,
        status,
        henkoulimit,
        adrs1,
        adrs2,
        adrs3,
        replace(
            replace((adrs4)::text, '−'::text, 'ー'::text),
            '－'::text,
            'ー'::text
        ),
        replace(
            replace((adrs5)::text, '−'::text, 'ー'::text),
            '－'::text,
            'ー'::text
        ),
        otodokesei,
        otodokemei,
        exec_time,
        usrid,
        sort_key
    ORDER BY sort_key,
        otodoke_day,
        {{encryption_1('usrid')}},
        webid
)
select * from final