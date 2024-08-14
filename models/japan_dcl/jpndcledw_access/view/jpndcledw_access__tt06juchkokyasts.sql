with tt06juchkokyasts as (
    select * from {{ ref('jpndcledw_integration__tt06juchkokyasts') }}
),

final as (
select
saleno as "saleno",
ruikaisu as "ruikaisu",
ruikingaku as "ruikingaku",
ruiindays as "ruiindays",
lastjuchdate as "lastjuchdate",
juchukeikadays as "juchukeikadays",
lastkonyudate as "lastkonyudate",
konyukeikadays as "konyukeikadays",
nenkaisu as "nenkaisu",
nenkingaku as "nenkingaku",
nenindays as "nenindays",
nengelryo as "nengelryo",
tsukigelryo as "tsukigelryo",
juchurkbncode as "juchurkbncode",
konyurkbncode as "konyurkbncode",
ruifkbncode as "ruifkbncode",
nenfkbncode as "nenfkbncode",
ruiikbncode as "ruiikbncode",
nenikbncode as "nenikbncode",
ruimkbncode as "ruimkbncode",
nenmkbncode1 as "nenmkbncode1",
nenmkbncode2 as "nenmkbncode2",
nenmkbncode3 as "nenmkbncode3",
nenmkbncode4 as "nenmkbncode4",
nenmkbncode5 as "nenmkbncode5",
tsukigkbncode as "tsukigkbncode",
segkbncode as "segkbncode",
insertdate as "insertdate",
inserttime as "inserttime",
insertid as "insertid",
updatedate as "updatedate",
updatetime as "updatetime",
updateid as "updateid",
kokyakbncode as "kokyakbncode",
bk_saleno as "bk_saleno",
inserted_date as "inserted_date",
inserted_by as "inserted_by",
updated_date as "updated_date",
updated_by as "updated_by"
from tt06juchkokyasts
)

select * from final