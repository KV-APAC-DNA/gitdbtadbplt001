with tm22kokyasts as (
    select * from {{ ref('jpndcledw_integration__tm22kokyasts') }}
),

final as (
select  
kokyano as "kokyano",
firstjuchdate as "firstjuchdate",
firstkonyudate as "firstkonyudate",
zaisekidays as "zaisekidays",
zaisekimonth as "zaisekimonth",
firsttsuhandate as "firsttsuhandate",
firsttenpodate as "firsttenpodate",
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
maindaibuncode as "maindaibuncode",
mainchubuncode as "mainchubuncode",
mainsyobuncode as "mainsyobuncode",
mainsaibuncode as "mainsaibuncode",
maintenpocode as "maintenpocode",
insertdate as "insertdate",
inserttime as "inserttime",
insertid as "insertid",
updatedate as "updatedate",
updatetime as "updatetime",
updateid as "updateid",
bk_kokyano as "bk_kokyano",
bk_maindaibuncode as "bk_maindaibuncode",
bk_mainchubuncode as "bk_mainchubuncode",
bk_mainsyobuncode as "bk_mainsyobuncode",
bk_mainsaibuncode as "bk_mainsaibuncode",
bk_firstjuchdate as "bk_firstjuchdate",
bk_firstkonyudate as "bk_firstkonyudate",
bk_firsttsuhandate as "bk_firsttsuhandate",
bk_firsttenpodate as "bk_firsttenpodate",
bk_zaisekidays as "bk_zaisekidays",
bk_zaisekimonth as "bk_zaisekimonth",
bk_ruikaisu as "bk_ruikaisu",
bk_ruikingaku as "bk_ruikingaku",
bk_ruiindays as "bk_ruiindays",
bk_nenkaisu as "bk_nenkaisu",
bk_nenkingaku as "bk_nenkingaku",
bk_nenindays as "bk_nenindays",
bk_nengelryo as "bk_nengelryo",
inserted_date as "inserted_date",
inserted_by as "inserted_by",
updated_date as "updated_date",
updated_by as "updated_by"
from tm22kokyasts
)

select * from final