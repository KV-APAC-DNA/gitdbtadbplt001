WITH tt02salem_add2_up_hen_mt
AS (
	SELECT *
	FROM dev_dna_core.snapjpdcledw_integration.tt02salem_add2_up_hen_mt
	)

	,tt02salem_add3_epi_hen_sub_mt
AS (
	SELECT *
	FROM dev_dna_core.snapjpdcledw_integration.tt02salem_add3_epi_hen_sub_mt
	)

    ,tt02salem_add1_exp_hen_mt
    as
    (
        select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.TT02SALEM_ADD1_EXP_HEN_MT
    )
    
	,transform_union1 
    AS
	(
		(
		SELECT (tt02salem_add1_exp_hen_mt.saleno)::CHARACTER VARYING AS saleno
			,tt02salem_add1_exp_hen_mt.gyono
			,tt02salem_add1_exp_hen_mt.meisaikbn
			,tt02salem_add1_exp_hen_mt.itemcode
			,tt02salem_add1_exp_hen_mt.suryo
			,tt02salem_add1_exp_hen_mt.tanka
			,tt02salem_add1_exp_hen_mt.kingaku
			,tt02salem_add1_exp_hen_mt.meisainukikingaku
			,tt02salem_add1_exp_hen_mt.wariritu
			,tt02salem_add1_exp_hen_mt.warimaekomitanka
			,tt02salem_add1_exp_hen_mt.warimaenukikingaku
			,tt02salem_add1_exp_hen_mt.warimaekomikingaku
			,tt02salem_add1_exp_hen_mt.dispsaleno
			,tt02salem_add1_exp_hen_mt.kesaiid
			,tt02salem_add1_exp_hen_mt.henpinsts
		FROM tt02salem_add1_exp_hen_mt
		WHERE (
				((tt02salem_add1_exp_hen_mt.henpinsts)::TEXT = ('3010'::CHARACTER VARYING)::TEXT)
				OR ((tt02salem_add1_exp_hen_mt.henpinsts)::TEXT = ('5020'::CHARACTER VARYING)::TEXT)
				)
		
		UNION ALL
		
		SELECT tt02salem_add2_up_hen_mt.saleno
			,tt02salem_add2_up_hen_mt.gyono
			,tt02salem_add2_up_hen_mt.meisaikbn
			,tt02salem_add2_up_hen_mt.itemcode
			,tt02salem_add2_up_hen_mt.suryo
			,tt02salem_add2_up_hen_mt.tanka
			,tt02salem_add2_up_hen_mt.kingaku
			,tt02salem_add2_up_hen_mt.meisainukikingaku
			,tt02salem_add2_up_hen_mt.wariritu
			,tt02salem_add2_up_hen_mt.warimaekomitanka
			,tt02salem_add2_up_hen_mt.warimaenukikingaku
			,tt02salem_add2_up_hen_mt.warimaekomikingaku
			,tt02salem_add2_up_hen_mt.dispsaleno
			,tt02salem_add2_up_hen_mt.kesaiid
			,tt02salem_add2_up_hen_mt.henpinsts
		FROM tt02salem_add2_up_hen_mt
		WHERE (
				((tt02salem_add2_up_hen_mt.henpinsts)::TEXT = ('3010'::CHARACTER VARYING)::TEXT)
				OR ((tt02salem_add2_up_hen_mt.henpinsts)::TEXT = ('5020'::CHARACTER VARYING)::TEXT)
				)
		)

UNION ALL

SELECT (tt02salem_add3_epi_hen_sub_mt.saleno)::CHARACTER VARYING AS saleno
	,tt02salem_add3_epi_hen_sub_mt.gyono
	,tt02salem_add3_epi_hen_sub_mt.meisaikbn
	,tt02salem_add3_epi_hen_sub_mt.itemcode
	,tt02salem_add3_epi_hen_sub_mt.suryo
	,tt02salem_add3_epi_hen_sub_mt.tanka
	,tt02salem_add3_epi_hen_sub_mt.kingaku
	,tt02salem_add3_epi_hen_sub_mt.meisainukikingaku
	,tt02salem_add3_epi_hen_sub_mt.wariritu
	,tt02salem_add3_epi_hen_sub_mt.warimaekomitanka
	,tt02salem_add3_epi_hen_sub_mt.warimaenukikingaku
	,tt02salem_add3_epi_hen_sub_mt.warimaekomikingaku
	,tt02salem_add3_epi_hen_sub_mt.dispsaleno
	,tt02salem_add3_epi_hen_sub_mt.kesaiid
	,tt02salem_add3_epi_hen_sub_mt.henpinsts
FROM tt02salem_add3_epi_hen_sub_mt
WHERE (
		((tt02salem_add3_epi_hen_sub_mt.henpinsts)::TEXT = ('3010'::CHARACTER VARYING)::TEXT)
		OR ((tt02salem_add3_epi_hen_sub_mt.henpinsts)::TEXT = ('5020'::CHARACTER VARYING)::TEXT)
		)

	) 
    SELECT * FROM transform_union1
