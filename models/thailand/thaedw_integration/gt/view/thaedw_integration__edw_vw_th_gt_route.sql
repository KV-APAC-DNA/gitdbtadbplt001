with itg_th_gt_route_detail as 
(
    select * from {{ref('thaitg_integration__itg_th_gt_route_detail')}}
),
itg_th_gt_route as 
(
    select * from {{ref('thaitg_integration__itg_th_gt_route')}}
),
final as 
(
    SELECT 
        rd.cntry_cd,
        rd.crncy_cd,
        rd.routeid AS route_id,
        rd.customerid AS store_id,
        rd.routeno,
        rd.saleunit AS distributor_id,
        rd.ship_to,
        rd.contact_person,
        rh.name AS route_name,
        rh.route_description,
        rh.isactive,
        rh.routesale AS sales_rep_id,
        rh.routecode,
        rh.description AS route_code_desc,
        CASE
            WHEN (
                left(trim((rd.customerid)::text), 1) = '_'::text
            ) THEN 'N'::text
            ELSE 'Y'::text
        END AS valid_flag,
        rd.flag AS route_detail_flag,
        rh.effective_start_date,
        rh.effective_end_date
    FROM itg_th_gt_route_detail rd
        LEFT JOIN itg_th_gt_route rh ON 
        (
            (
                (
                    ((rd.routeid)::text = (rh.routeid)::text)
                    AND (
                        upper((rd.saleunit)::text) = upper((rh.saleunit)::text)
                    )
                )
                AND 
                (
                    CASE
                        WHEN (
                            (upper((rd.flag)::text) <> 'D'::text)
                            AND (
                                (rd.created_date)::date <= (rh.effective_start_date)::date
                            )
                        ) 
                        THEN 1
                        WHEN (
                            (
                                (upper((rd.flag)::text) = 'D'::text)
                                AND (
                                    (rd.created_date)::date <= (rh.effective_start_date)::date
                                )
                            )
                            AND (
                                (rd.last_modified_date)::date > (rh.effective_start_date)::date
                            )
                        )
                        THEN 1
                        ELSE 0
                    END = 1
                )
            )
        )
)
select * from final