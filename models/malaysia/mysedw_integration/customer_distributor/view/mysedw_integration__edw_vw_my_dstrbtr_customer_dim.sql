with itg_my_dstrbtr_cust_dim as
(
    select * from {{ source('mysitg_integration', 'itg_my_dstrbtr_cust_dim') }}
),
itg_my_dstrbtrr_dim as
(
    select * from {{ source('mysitg_integration', 'itg_my_dstrbtrr_dim') }}
),
itg_my_customer_dim as
(
    select * from {{ source('mysitg_integration', 'itg_my_customer_dim') }}
),
final as
(
    select
        'MY' as cntry_cd,
        'Malaysia' as cntry_nm,
        imcd.dstrbtr_grp_cd,
        null as dstrbtr_soldto_code,
        imoa.cust_id as sap_soldto_code,
        imoa.outlet_id as cust_cd,
        imoa.outlet_desc as cust_nm,
        null as alt_cust_cd,
        null as alt_cust_nm,
        null as addr,
        null as area_cd,
        null as area_nm,
        null as state_cd,
        null as state_nm,
        null as region_cd,
        imdd.region as region_nm,
        null as prov_cd,
        null as prov_nm,
        null as town_cd,
        imoa.town as town_nm,
        null as city_cd,
        null as city_nm,
        null as post_cd,
        null as post_nm,
        imoa.slsmn_cd,
        null as slsmn_nm,
        null as chnl_cd,
        imoa.outlet_type1 as chnl_desc,
        null as sub_chnl_cd,
        imoa.outlet_type2 as sub_chnl_desc,
        null as chnl_attr1_cd,
        imoa.outlet_type3 as chnl_attr1_desc,
        null as chnl_attr2_cd,
        imoa.outlet_type4 as chnl_attr2_desc,
        null as outlet_type_cd,
        cast(
                (
                (
                    (
                    (
                        (
                        (
                            (
                                cast((imoa.outlet_type1) as text) || cast((cast('' as varchar)) as text)
                            ) ||cast((imoa.outlet_type2) as text) 
                        ) || cast((cast('' as varchar)) as text)
                        ) || cast((imoa.outlet_type3) as text)
                    ) || cast((cast('' as varchar)) as text)
                    ) || cast((imoa.outlet_type4) as text)
                )
            ) as varchar
        ) as outlet_type_desc,
        null as cust_grp_cd,
        null as cust_grp_desc,
        null as cust_grp_attr1_cd,
        null as cust_grp_attr1_desc,
        null as cust_grp_attr2_cd,
        null as cust_grp_attr2_desc,
        null as sls_dstrct_cd,
        null as sls_dstrct_nm,
        null as sls_office_cd,
        null as sls_office_desc,
        null as sls_grp_cd,
        null as sls_grp_desc,
        null as status
    from 
    (
        (
            itg_my_dstrbtr_cust_dim as imoa
            left join itg_my_customer_dim as imcd
                on ((  ltrim(cast((    imcd.cust_id  ) as text))  = ltrim(cast((    imoa.cust_id  ) as text)))
                )
        )
        left join itg_my_dstrbtrr_dim as imdd
            on ((  ltrim(cast((    imdd.cust_id  ) as text)) = ltrim(cast((    imoa.cust_id  ) as text)))
            )
    )
)
select * from final