WITH source AS
(
    SELECT * FROM {{ ref('jpnwks_integration__wk_so_planet_no_dup') }}
),

transformed AS
(
    SELECT 
        a.jcp_rec_seq,
        a.item_cd_typ,
        CASE 
            WHEN a.item_cd_typ IN ('J', 'K', 'S')
                THEN '1'
            WHEN a.item_cd_typ = 'I'
                THEN '2'
            ELSE '0'
            END v_f_item_cd
    FROM source a
),

final as
(
    SELECT
        jcp_rec_seq::NUMBER(10,0) AS jcp_rec_seq,
        item_cd_typ::VARCHAR(20) AS item_cd_typ,
        v_f_item_cd::VARCHAR(10) AS v_f_item_cd
    FROM transformed
)

SELECT * FROM final