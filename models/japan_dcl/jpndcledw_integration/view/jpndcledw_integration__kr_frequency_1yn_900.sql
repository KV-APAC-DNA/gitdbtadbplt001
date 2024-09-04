with kr_frequency_1yn_900_kizuna as (
select * from {{ ref('jpndcledw_integration__kr_frequency_1yn_900_kizuna') }}
),

final as
(
    SELECT 
        kr_frequency_1yn_900_kizuna.saleno, 
        kr_frequency_1yn_900_kizuna.kokyano, 
        kr_frequency_1yn_900_kizuna.now_rowno, 
        kr_frequency_1yn_900_kizuna.pre_rowno, 
        kr_frequency_1yn_900_kizuna.now_juchdate, 
        kr_frequency_1yn_900_kizuna.pre_juchdate, 
        kr_frequency_1yn_900_kizuna.elapsed, 
        kr_frequency_1yn_900_kizuna.insertdate, 
        kr_frequency_1yn_900_kizuna.shukadate_p 
    FROM kr_frequency_1yn_900_kizuna
)
SELECT * FROM final