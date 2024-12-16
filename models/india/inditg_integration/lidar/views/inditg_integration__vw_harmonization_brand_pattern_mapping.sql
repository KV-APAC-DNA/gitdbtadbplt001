with brand_lkp as (
    select * from {{ source('mds_access','mds_lkp_brand_hierarchy') }}
),

final as (
    select
        name::VARCHAR(500) as brand,
        name::VARCHAR(500) as p1,
        lower(name)::VARCHAR(500) as p2,
        upper(name)::VARCHAR(1500) as p3,
        ('-' || name || '-')::VARCHAR(502) as p5,
        (' ' || name)::VARCHAR(501) as p8,
        ('_' || name)::VARCHAR(501) as p14,
        ('|' || name || '|')::VARCHAR(502) as p42,
        (' ' || name || ' ')::VARCHAR(502) as p52,
        lower(name || ' ')::VARCHAR(501) as p64,
        ('_' || name || '_')::VARCHAR(502) as p65,
        lower('_' || name || '_')::VARCHAR(502) as p73,
        (name || ' ')::VARCHAR(501) as p80,
        upper(name || ' ')::VARCHAR(1503) as p81,
        gpch_brand_name::VARCHAR(250) as gcph_brand_name
    from brand_lkp
)

select * from final
