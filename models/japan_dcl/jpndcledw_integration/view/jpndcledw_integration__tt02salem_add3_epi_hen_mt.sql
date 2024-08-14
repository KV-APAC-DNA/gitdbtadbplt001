with tt02salem_add3_epi_hen_sub_mt as (
    select * from {{ ref('jpndcledw_integration__tt02salem_add3_epi_hen_sub_mt') }}
),

final as (
SELECT tt02salem_add3_epi_hen_sub_mt.saleno,
    tt02salem_add3_epi_hen_sub_mt.gyono,
    tt02salem_add3_epi_hen_sub_mt.meisaikbn,
    tt02salem_add3_epi_hen_sub_mt.itemcode,
    sum(tt02salem_add3_epi_hen_sub_mt.suryo) AS suryo,
    tt02salem_add3_epi_hen_sub_mt.tanka,
    sum(tt02salem_add3_epi_hen_sub_mt.kingaku) AS kingaku,
    cast(cast(sum(tt02salem_add3_epi_hen_sub_mt.meisainukikingaku) as integer) as float) AS meisainukikingaku,
    tt02salem_add3_epi_hen_sub_mt.wariritu,
    sum(tt02salem_add3_epi_hen_sub_mt.warimaekomitanka) AS warimaekomitanka,
    sum(tt02salem_add3_epi_hen_sub_mt.warimaenukikingaku) AS warimaenukikingaku,
    sum(tt02salem_add3_epi_hen_sub_mt.warimaekomikingaku) AS warimaekomikingaku,
    tt02salem_add3_epi_hen_sub_mt.dispsaleno,
    tt02salem_add3_epi_hen_sub_mt.kesaiid,
    tt02salem_add3_epi_hen_sub_mt.henpinsts
FROM tt02salem_add3_epi_hen_sub_mt
GROUP BY tt02salem_add3_epi_hen_sub_mt.saleno,
    tt02salem_add3_epi_hen_sub_mt.gyono,
    tt02salem_add3_epi_hen_sub_mt.meisaikbn,
    tt02salem_add3_epi_hen_sub_mt.itemcode,
    tt02salem_add3_epi_hen_sub_mt.tanka,
    tt02salem_add3_epi_hen_sub_mt.wariritu,
    tt02salem_add3_epi_hen_sub_mt.dispsaleno,
    tt02salem_add3_epi_hen_sub_mt.kesaiid,
    tt02salem_add3_epi_hen_sub_mt.henpinsts
)

select * from final