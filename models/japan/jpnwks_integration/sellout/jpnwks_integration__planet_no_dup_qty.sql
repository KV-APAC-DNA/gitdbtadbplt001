WITH source AS
(
    SELECT * FROM {{ ref('jpnwks_integration__wk_so_planet_no_dup') }}
),

A AS
(
    SELECT 
        A.JCP_REC_SEQ,
        A.QTY,
        CASE 
            WHEN SUBSTRING(A.QTY, LENGTH(A.QTY), 1) IN ('{', '}')
                THEN '0'
            WHEN SUBSTRING(A.QTY, LENGTH(A.QTY), 1) IN ('A', 'J')
                THEN '1'
            WHEN SUBSTRING(A.QTY, LENGTH(A.QTY), 1) IN ('B', 'K')
                THEN '2'
            WHEN SUBSTRING(A.QTY, LENGTH(A.QTY), 1) IN ('C', 'L')
                THEN '3'
            WHEN SUBSTRING(A.QTY, LENGTH(A.QTY), 1) IN ('D', 'M')
                THEN '4'
            WHEN SUBSTRING(A.QTY, LENGTH(A.QTY), 1) IN ('E', 'N')
                THEN '5'
            WHEN SUBSTRING(A.QTY, LENGTH(A.QTY), 1) IN ('F', 'O')
                THEN '6'
            WHEN SUBSTRING(A.QTY, LENGTH(A.QTY), 1) IN ('G', 'P')
                THEN '7'
            WHEN SUBSTRING(A.QTY, LENGTH(A.QTY), 1) IN ('H', 'Q')
                THEN '8'
            WHEN SUBSTRING(A.QTY, LENGTH(A.QTY), 1) IN ('I', 'R')
                THEN '9'
            ELSE SUBSTRING(A.QTY, LENGTH(A.QTY), 1)
            END V_CHGCHAR,
        CASE 
            WHEN SUBSTRING(A.QTY, LENGTH(A.QTY), 1) IN ('{', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I')
                THEN NULL
            WHEN SUBSTRING(A.QTY, LENGTH(A.QTY), 1) IN ('}', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R')
                THEN '-'
            ELSE NULL
            END V_SIGN
    FROM source A
    WHERE LEN(RTRIM(LTRIM(A.QTY))) != 0
),

trns as
(
    SELECT 
        A.JCP_REC_SEQ,
        A.QTY,
        SUBSTRING(A.QTY, 1, LENGTH(QTY) - 1) || A.V_CHGCHAR AS V_CHGNUM
    FROM  A
),

final AS
(
    SELECT
        JCP_REC_SEQ::NUMBER(10,0) AS JCP_REC_SEQ,
        QTY::VARCHAR(256) AS QTY,
        V_CHGNUM::VARCHAR(256) AS V_CHGNUM
    FROM trns
)

SELECT * FROM final
