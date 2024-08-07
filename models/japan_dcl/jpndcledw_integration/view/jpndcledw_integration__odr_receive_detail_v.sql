WITH tbecordermeisai AS
(
    SELECT * FROM dev_dna_core.snapjpdclitg_integration.tbecordermeisai
),

final AS
(
    SELECT tbecordermeisai.diorderid AS odrreceiveno,
        tbecordermeisai.dimeisaiid AS odrreceiveseqno,
        tbecordermeisai.dsitemid AS itemcode,
        tbecordermeisai.diusualprc AS price,
        (tbecordermeisai.ditotalprc - abs(tbecordermeisai.c_didiscountmeisai)) AS discountpriceinctax,
        tbecordermeisai.diitemnum AS quantity
    FROM tbecordermeisai
)

SELECT * FROM final