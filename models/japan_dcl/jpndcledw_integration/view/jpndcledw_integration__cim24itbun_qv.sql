WITH cim24itbun AS
(
    SELECT * FROM dev_dna_core.snapjpdcledw_integration.cim24itbun
),

cim24itbun_add_qv AS
(
    SELECT * FROM dev_dna_core.snapjpdcledw_integration.cim24itbun_add_qv
),


ct1 AS
(
    SELECT cim24itbun.itbunshcode,
        cim24itbun.itbuncode,
        cim24itbun.itbunname
    FROM cim24itbun 
),

ct2 AS
(
    SELECT cim24itbun_add_qv.itbunshcode,
        cim24itbun_add_qv.itbuncode,
        cim24itbun_add_qv.itbunname
    FROM cim24itbun_add_qv 
),

final AS
(
    SELECT * FROM ct1
    union ALL
    SELECT * FROM ct2
)

SELECT * FROM final