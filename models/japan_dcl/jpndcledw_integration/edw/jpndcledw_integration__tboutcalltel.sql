with tbOutcallResult_wk as (
    select * from dev_dna_core.jpdcledw_integration.tbOutcallResult_wk
),
base_dataset as(
    SELECT 
        outcall_result.diUsrId,
        LPAD(outcall_result.diUsrId,10,'0') diUsrId10,
        {{ encryption_1("diUsrId10") }} as diusrid_encrypted,
        outcall_result.mainTel,
        outcall_result.dsSeibetsu,
        CASE
            WHEN outcall_result.age BETWEEN 35 AND 60               THEN 1
            WHEN outcall_result.age < 35 OR outcall_result.age > 60 THEN 2
            WHEN outcall_result.age = 99999                         THEN 3
            ELSE 3
        END age,
        CASE
            WHEN outcall_result.dsdat9 IN ('給料事務職','給料労務職','役員管理職','自由業','商工自営業','農林漁業') THEN '00000'
            WHEN outcall_result.dsdat9 IN ('主婦専業') THEN '00010'
            ELSE '99999'
        END occupation,
        '' pnt,
        '' excflg
    FROM tbOutcallResult_wk outcall_result
    INNER JOIN (
            SELECT 
                mainTel
            FROM tbOutcallResult_wk
            WHERE excflg = '0'
            GROUP BY 
                mainTel
            HAVING COUNT(mainTel) > 1
        ) filtered_set
        ON outcall_result.mainTel = filtered_set.mainTel
    WHERE 
        outcall_result.excflg = '0'
),
pnt_1_excflg_0 as (
    SELECT 
        diUsrId,
        outcall_tel2.mainTel
    FROM base_dataset outcall_tel2
    INNER JOIN (
        SELECT 
            mainTel,
            dsSeibetsu
        FROM base_dataset
        WHERE dsSeibetsu = '1'
        GROUP BY
            mainTel,
            dsSeibetsu
        HAVING COUNT(dsSeibetsu) = '1'
    )   telnum_dupl
    ON outcall_tel2.mainTel = telnum_dupl.mainTel
    AND outcall_tel2.dsSeibetsu = telnum_dupl.dsSeibetsu
),
pnt_0_excflg_12 as(
    SELECT 
        outcall_tel2.diUsrId,
        outcall_tel2.mainTel
    FROM base_dataset outcall_tel2
    INNER JOIN (
                SELECT mainTel,
                        dsSeibetsu
                FROM base_dataset
                GROUP BY mainTel,
                        dsSeibetsu
                HAVING dsSeibetsu = '1'
                    AND COUNT(dsSeibetsu) > '1'
                )telnum_dupl
        ON outcall_tel2.mainTel = telnum_dupl.mainTel
    WHERE outcall_tel2.dsSeibetsu <> '1'
),
pnt_2_excflg_0 as(
    SELECT 
        outcall_tel2.diUsrId,
        outcall_tel2.mainTel
    FROM base_dataset outcall_tel2
    INNER JOIN (
        SELECT 
            mainTel,
            age
        FROM base_dataset
        GROUP BY 
            mainTel,
            age
        HAVING 
            age = '1' 
            AND COUNT(age) = '1'
        ) telnum_dupl
    ON outcall_tel2.mainTel = telnum_dupl.mainTel
    AND outcall_tel2.age = telnum_dupl.age
),
pnt_0_excflg_22 as (
    SELECT 
        outcall_tel2.diUsrId,
        outcall_tel2.mainTel
    FROM base_dataset outcall_tel2
    INNER JOIN (
        SELECT 
            mainTel,
            age
        FROM base_dataset
        GROUP BY 
            mainTel,
            age
        HAVING 
            age = '1'
            AND COUNT(age) > '1'
        ) telnum_dupl
    ON outcall_tel2.mainTel = telnum_dupl.mainTel
    WHERE outcall_tel2.age <> '1'             
),

pnt_3_excflg_0 as(
    SELECT 
        outcall_tel2.diUsrId,
        outcall_tel2.mainTel
    FROM base_dataset outcall_tel2
    INNER JOIN (
                SELECT 
                    mainTel,
                    occupation
                FROM base_dataset
                WHERE (excflg IS NULL or excflg = '')
                GROUP BY 
                    mainTel,
                    occupation
                HAVING occupation = '00010' 
                    AND COUNT(occupation) = '1'
                ) telnum_dupl
        ON outcall_tel2.mainTel = telnum_dupl.mainTel
        AND outcall_tel2.occupation = telnum_dupl.occupation
        AND (excflg IS NULL or excflg = '')
),
pnt_0_excflg_32 as(
    SELECT 
        outcall_tel2.diUsrId,
        outcall_tel2.mainTel
    FROM base_dataset outcall_tel2
    INNER JOIN (
                SELECT 
                    mainTel,
                    occupation
                FROM base_dataset
                    GROUP BY
                    mainTel,
                    occupation
                HAVING 
                    occupation = '00010'
                    AND COUNT(occupation) > '1'
                )telnum_dupl
        ON outcall_tel2.mainTel = telnum_dupl.mainTel
    WHERE outcall_tel2.occupation <> '00010'
),
pnt_4_excflg_0 as (
    SELECT 
        outcall_tel2.diUsrId,
        outcall_tel2.mainTel
    FROM base_dataset outcall_tel2
    INNER JOIN (
        SELECT 
            mainTel,
            occupation
        FROM base_dataset
        WHERE 
            (excflg IS NULL or excflg = '')
        GROUP BY 
            mainTel,
            occupation
        HAVING 
            occupation = '00000' 
            AND COUNT(occupation) = '1'
    )telnum_dupl
    ON outcall_tel2.mainTel = telnum_dupl.mainTel
    AND outcall_tel2.occupation = telnum_dupl.occupation
),
pnt_0_excflg_42 as(
    SELECT 
        outcall_tel2.diUsrId,
        outcall_tel2.mainTel
    FROM base_dataset outcall_tel2
    INNER JOIN (
            SELECT 
                mainTel,
                occupation
            FROM base_dataset
            GROUP BY 
                mainTel,
                occupation
            HAVING 
                occupation = '00000'
                AND COUNT(occupation) > '00000'
    )telnum_dupl
    ON outcall_tel2.mainTel = telnum_dupl.mainTel
    WHERE outcall_tel2.occupation <> '00000'
),
pnt_0_excflg_51 as(
    SELECT 
        diusrid_encrypted
    FROM base_dataset outcall_tel2
    INNER JOIN (
            SELECT 
                mainTel
            FROM base_dataset
            WHERE (excflg IS NULL or excflg = '')
            GROUP BY 
                mainTel
            HAVING COUNT(mainTel) > '1'
        )telnum_dupl
        ON outcall_tel2.mainTel = telnum_dupl.mainTel
        AND (excflg IS NULL or excflg = '')
    MINUS
    SELECT 
        min(diusrid_encrypted)
    FROM base_dataset outcall_tel2
    WHERE 
        (excflg IS NULL or excflg = '') 
    GROUP BY 
        mainTel
),
transformed as (
    select
        f1.diUsrId,
        f1.mainTel,
        f1.dsSeibetsu,
        f1.age,
        f1.occupation,
        case 
            when p1e0.diUsrId is not null then '1' 
            when p1e0_2.mainTel is not null then '0' 
            when p0e12.mainTel is not null then '0' 
            when p2e0.diUsrId is not null then '2' 
            when p2e0_2.mainTel is not null then '0' 
            when p0e22.mainTel is not null then '0' 
            when p3e0.diUsrId is not null then '3' 
            when p3e0_2.mainTel is not null then '0' 
            when p0e32.mainTel is not null then '0' 
            when p4e0.diUsrId is not null then '4' 
            when p4e0_2.mainTel is not null then '0' 
            when p0e42.mainTel is not null then '0' 
            when p0e51.diUsrId_encrypted is not null then '0' 
            else '5'            
        end as pnt,
        case 
            when p1e0.diUsrId is not null then '0' 
            when p1e0_2.mainTel is not null then '11'
            when p0e12.mainTel is not null then '12'
            when p2e0.diUsrId is not null then '0' 
            when p2e0_2.mainTel is not null then '21' 
            when p0e22.mainTel is not null then  '22' 
            when p3e0.diUsrId is not null then '0' 
            when p3e0_2.mainTel is not null then '31' 
            when p0e32.mainTel is not null then '32' 
            when p4e0.diUsrId is not null then '0' 
            when p4e0_2.mainTel is not null then '41' 
            when p0e42.mainTel is not null then '42' 
            when p0e51.diUsrId_encrypted is not null then '51' 
            else '0' 
        end as excflg
    from base_dataset f1
    left join pnt_1_excflg_0 p1e0 on f1.diUsrId = p1e0.diUsrId
    left join (select distinct mainTel from pnt_1_excflg_0) p1e0_2
        on f1.mainTel = p1e0_2.mainTel and p1e0.diUsrId is null
    left join pnt_0_excflg_12 p0e12 
        on f1.diUsrId = p0e12.diUsrId
        and f1.mainTel = p0e12.mainTel
        and p1e0_2.mainTel is null
    left join pnt_2_excflg_0 p2e0 on f1.diUsrId = p2e0.diUsrId and p0e12.diUsrId is null
    left join (select distinct mainTel from pnt_2_excflg_0) p2e0_2
        on f1.mainTel = p2e0_2.mainTel and p2e0.diUsrId is null
    left join pnt_0_excflg_22 p0e22 
        on f1.diUsrId = p0e22.diUsrId
        and f1.mainTel = p0e22.mainTel
        and p2e0_2.mainTel is null
    left join pnt_3_excflg_0 p3e0 on f1.diUsrId = p3e0.diUsrId  and p0e22.diUsrId is null
    left join (select distinct mainTel from pnt_3_excflg_0) p3e0_2
        on f1.mainTel = p3e0_2.mainTel and p3e0.diUsrId is null
    left join pnt_0_excflg_32 p0e32 
        on f1.diUsrId = p0e32.diUsrId
        and f1.mainTel = p0e32.mainTel
        and p3e0_2.mainTel is null
    left join pnt_4_excflg_0 p4e0 on f1.diUsrId = p4e0.diUsrId and p0e32.diUsrId is null
    left join (select distinct mainTel from pnt_4_excflg_0) p4e0_2
        on f1.mainTel = p4e0_2.mainTel and p4e0.diUsrId is null
    left join pnt_0_excflg_42 p0e42 
        on f1.diUsrId = p0e42.diUsrId
        and f1.mainTel = p0e42.mainTel
        and p4e0_2.mainTel is null
    left join pnt_0_excflg_51 p0e51 on f1.diusrid_encrypted = p0e51.diusrid_encrypted and p0e42.diUsrId is null
),
final as (
    select
        diusrid::number(38,0)    as diusrid,
        maintel::varchar(6000)    as maintel,
        dsseibetsu::varchar(1)    as dsseibetsu,
        age::number(18,0)    as age,
        occupation::varchar(5)    as occupation,
        pnt::varchar(1)    as pnt,
        excflg::varchar(2)    as excflg
    from transformed
)
select * from final
