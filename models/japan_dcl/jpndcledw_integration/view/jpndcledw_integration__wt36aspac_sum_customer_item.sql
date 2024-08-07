WITH aspac_common_new AS
(
    SELECT * FROM dev_dna_core.snapjpdcledw_integration.aspac_common_new
),

final AS
(
    SELECT aspac_common_new.sum_name,
        aspac_common_new.itemcode,
        aspac_common_new.itemname,
        aspac_common_new.itemkbn,
        aspac_common_new.itemkbnname,
        aspac_common_new.tokuicode,
        aspac_common_new.tokuiname,
        aspac_common_new.tokuiname_ryaku,
        aspac_common_new.tokuiname2,
        aspac_common_new.suryo,
        aspac_common_new.kingaku,
        aspac_common_new.shimebi,
        aspac_common_new.nohindate,
        aspac_common_new.shukadate,
        aspac_common_new.torikeikbn,
        aspac_common_new.salekeijodate
    FROM aspac_common_new
)

SELECT * FROM final