with edw_customer_attr_hier_dim as (
    select * from snapaspedw_integration.edw_customer_attr_hier_dim
),
hk as (
    SELECT DISTINCT edw_customer_attr_hier_dim.cntry,
        edw_customer_attr_hier_dim.aw_remote_key AS sold_to_party,
        edw_customer_attr_hier_dim.channel,
        edw_customer_attr_hier_dim.sls_grp,
        edw_customer_attr_hier_dim.cust_hier_l1,
        edw_customer_attr_hier_dim.cust_hier_l2,
        edw_customer_attr_hier_dim.cust_hier_l3,
        edw_customer_attr_hier_dim.cust_hier_l4,
        edw_customer_attr_hier_dim.cust_hier_l5
    FROM edw_customer_attr_hier_dim
    WHERE (
            (edw_customer_attr_hier_dim.cntry)::text = ('HongKong'::character varying)::text
        )
),
kr as (
    SELECT DISTINCT edw_customer_attr_hier_dim.cntry,
        edw_customer_attr_hier_dim.aw_remote_key AS sold_to_party,
        edw_customer_attr_hier_dim.channel,
        edw_customer_attr_hier_dim.sls_grp,
        edw_customer_attr_hier_dim.cust_hier_l1,
        edw_customer_attr_hier_dim.cust_hier_l2,
        edw_customer_attr_hier_dim.cust_hier_l3,
        edw_customer_attr_hier_dim.cust_hier_l4,
        edw_customer_attr_hier_dim.cust_hier_l5
    FROM edw_customer_attr_hier_dim
    WHERE (
            (edw_customer_attr_hier_dim.cntry)::text = ('Korea'::character varying)::text
        )
),
tw as (
    SELECT DISTINCT edw_customer_attr_hier_dim.cntry,
        edw_customer_attr_hier_dim.aw_remote_key AS sold_to_party,
        edw_customer_attr_hier_dim.channel,
        edw_customer_attr_hier_dim.sls_grp,
        (
            "max"((edw_customer_attr_hier_dim.cust_hier_l1)::text)
        )::character varying AS cust_hier_l1,
        (
            "max"((edw_customer_attr_hier_dim.cust_hier_l2)::text)
        )::character varying AS cust_hier_l2,
        (
            "max"((edw_customer_attr_hier_dim.cust_hier_l3)::text)
        )::character varying AS cust_hier_l3,
        (
            "max"((edw_customer_attr_hier_dim.cust_hier_l4)::text)
        )::character varying AS cust_hier_l4,
        (
            "max"((edw_customer_attr_hier_dim.cust_hier_l5)::text)
        )::character varying AS cust_hier_l5
    FROM edw_customer_attr_hier_dim
    WHERE (
            (edw_customer_attr_hier_dim.cntry)::text = ('Taiwan'::character varying)::text
        )
    GROUP BY edw_customer_attr_hier_dim.cntry,
        edw_customer_attr_hier_dim.aw_remote_key,
        edw_customer_attr_hier_dim.channel,
        edw_customer_attr_hier_dim.sls_grp
),
derived_table1 as (
    select * from hk
    union all
    select * from kr
    union all
    select * from tw
),
final as (
    SELECT derived_table1.cntry,
        derived_table1.sold_to_party,
        derived_table1.channel,
        derived_table1.sls_grp,
        derived_table1.cust_hier_l1,
        derived_table1.cust_hier_l2,
        derived_table1.cust_hier_l3,
        derived_table1.cust_hier_l4,
        derived_table1.cust_hier_l5
    FROM derived_table1
)
select * from final