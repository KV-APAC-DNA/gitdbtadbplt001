with kr_frequency_1yn_900 as (
    select * from {{ ref('jpndcledw_integration__kr_frequency_1yn_900') }}
),

final as (
select  
saleno as "saleno",
kokyano as "kokyano",
now_rowno as "now_rowno",
pre_rowno as "pre_rowno",
now_juchdate as "now_juchdate",
pre_juchdate as "pre_juchdate",
elapsed as "elapsed",
insertdate as "insertdate",
shukadate_p as "shukadate_p"
from kr_frequency_1yn_900
)

select * from final