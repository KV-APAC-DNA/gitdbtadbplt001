with 
itg_th_dstrbtr_customer_dim as 
(
    select * from  {{ ref('thaitg_integration__itg_th_dstrbtr_customer_dim') }}
),
final as (
    select 
        'TH' as cntry_cd
        ,'Thailand' as cntry_nm
        ,c.dstrbtr_id as dstrbtr_grp_cd
        ,null as dstrbtr_soldto_code
        ,(ltrim((c.dstrbtr_cd)::text, ((0)::character varying)::text))::character varying as sap_soldto_code
        ,(ltrim((c.old_cust_id)::text, ((0)::character varying)::text))::character varying as cust_cd
        ,c.ar_nm as cust_nm
        ,(ltrim((c.ar_cd)::text, ((0)::character varying)::text))::character varying as alt_cust_cd
        ,null as alt_cust_nm
        ,c.ar_adres as addr
        ,null as area_cd
        ,null as area_nm
        ,null as state_cd
        ,null as state_nm
        ,c.region as region_cd
        ,c.region_desc as region_nm
        ,c.bill_prvnce as prov_cd
        ,c.sls_dist_city_eng as prov_nm
        ,null as town_cd
        ,null as town_nm
        ,c.bill_dist as city_cd
        ,c.dist_nm as city_nm
        ,c.bill_zip_cd as post_cd
        ,null as post_nm
        ,null as slsmn_cd
        ,null as slsmn_nm
        ,case 
            when (
                    "substring" (
                        (c.sls_grp)::text
                        ,2
                        ,1
                        ) = ('1'::character varying)::text
                    )
                then '1010'::character varying
            else '1020'::character varying
            end as chnl_cd
        ,case 
            when (
                    "substring" (
                        (c.sls_grp)::text
                        ,2
                        ,1
                        ) = ('1'::character varying)::text
                    )
                then 'Van Channel'::character varying
            else 'Credit Channel'::character varying
            end as chnl_desc
        ,null as sub_chnl_cd
        ,null as sub_chnl_desc
        ,null as chnl_attr1_cd
        ,null as chnl_attr1_desc
        ,null as chnl_attr2_cd
        ,null as chnl_attr2_desc
        ,c.ar_typ_cd as outlet_type_cd
        ,c.grp_nm as outlet_type_desc
        ,case 
            when ((c.cust_type)::text = ('KA'::character varying)::text)
                then 'BRONZE'::character varying
            else case 
                    when ((c.cust_type)::text = ('PL'::character varying)::text)
                        then 'PLATINUM'::character varying
                    else case 
                            when ((c.cust_type)::text = ('GD'::character varying)::text)
                                then 'GOLD'::character varying
                            else case 
                                    when ((c.cust_type)::text = ('SV'::character varying)::text)
                                        then 'SILVER'::character varying
                                    else case 
                                            when (
                                                    "substring" (
                                                        (c.sls_grp)::text
                                                        ,2
                                                        ,1
                                                        ) = ('1'::character varying)::text
                                                    )
                                                then 'VAN'::character varying
                                            else 'BRONZE'::character varying
                                            end
                                    end
                            end
                    end
            end as cust_grp_cd
        ,case 
            when ((c.cust_type)::text = ('KA'::character varying)::text)
                then 'BRONZE'::character varying
            else case 
                    when ((c.cust_type)::text = ('PL'::character varying)::text)
                        then 'PLATINUM'::character varying
                    else case 
                            when ((c.cust_type)::text = ('GD'::character varying)::text)
                                then 'GOLD'::character varying
                            else case 
                                    when ((c.cust_type)::text = ('SV'::character varying)::text)
                                        then 'SILVER'::character varying
                                    else case 
                                            when (
                                                    "substring" (
                                                        (c.sls_grp)::text
                                                        ,2
                                                        ,1
                                                        ) = ('1'::character varying)::text
                                                    )
                                                then 'VAN'::character varying
                                            else 'BRONZE'::character varying
                                            end
                                    end
                            end
                    end
            end as cust_grp_desc
        ,null as cust_grp_attr1_cd
        ,null as cust_grp_attr1_desc
        ,null as cust_grp_attr2_cd
        ,null as cust_grp_attr2_desc
        ,c.sls_dist as sls_dstrct_cd
        ,c.sls_dist_city_eng as sls_dstrct_nm
        ,(
            case 
                when (
                        "substring" (
                            (c.sls_grp)::text
                            ,2
                            ,1
                            ) = ('1'::character varying)::text
                        )
                    then (
                            "substring" (
                                (c.sls_grp)::text
                                ,1
                                ,1
                                ) || ('1010'::character varying)::text
                            )
                else (
                        "substring" (
                            (c.sls_grp)::text
                            ,1
                            ,1
                            ) || ('1020'::character varying)::text
                        )
                end
            )::character varying as sls_office_cd
        ,null as sls_office_desc
        ,c.sls_grp as sls_grp_cd
        ,null as sls_grp_desc
        ,(c.actv_status)::character varying as status
    from itg_th_dstrbtr_customer_dim c
    )

select * from final