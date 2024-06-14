with edw_customer_attr_hier_dim as(
	select * from {{ ref('aspedw_integration__edw_customer_attr_hier_dim') }}
),
union1 as(
	SELECT DISTINCT edw_customer_attr_hier_dim.cntry
		,edw_customer_attr_hier_dim.aw_remote_key AS sold_to_party
		,edw_customer_attr_hier_dim.channel
		,edw_customer_attr_hier_dim.sls_grp
		,edw_customer_attr_hier_dim.cust_hier_l1
		,edw_customer_attr_hier_dim.cust_hier_l2
		,edw_customer_attr_hier_dim.cust_hier_l3
		,edw_customer_attr_hier_dim.cust_hier_l4
		,edw_customer_attr_hier_dim.cust_hier_l5
		,edw_customer_attr_hier_dim.store_typ
	FROM edw_customer_attr_hier_dim
	WHERE ((edw_customer_attr_hier_dim.cntry)::TEXT = ('HongKong'::CHARACTER VARYING)::TEXT)
),
union2 as(
	SELECT DISTINCT edw_customer_attr_hier_dim.cntry
		,edw_customer_attr_hier_dim.aw_remote_key AS sold_to_party
		,edw_customer_attr_hier_dim.channel
		,edw_customer_attr_hier_dim.sls_grp
		,("max" ((edw_customer_attr_hier_dim.cust_hier_l1)::TEXT))::CHARACTER VARYING AS cust_hier_l1
		,("max" ((edw_customer_attr_hier_dim.cust_hier_l2)::TEXT))::CHARACTER VARYING AS cust_hier_l2
		,("max" ((edw_customer_attr_hier_dim.cust_hier_l3)::TEXT))::CHARACTER VARYING AS cust_hier_l3
		,("max" ((edw_customer_attr_hier_dim.cust_hier_l4)::TEXT))::CHARACTER VARYING AS cust_hier_l4
		,("max" ((edw_customer_attr_hier_dim.cust_hier_l5)::TEXT))::CHARACTER VARYING AS cust_hier_l5
		,edw_customer_attr_hier_dim.store_typ
	FROM edw_customer_attr_hier_dim
	WHERE (
			((edw_customer_attr_hier_dim.cntry)::TEXT = ('Taiwan'::CHARACTER VARYING)::TEXT)
			OR ((edw_customer_attr_hier_dim.cntry)::TEXT = ('Korea'::CHARACTER VARYING)::TEXT)
			)
	GROUP BY edw_customer_attr_hier_dim.cntry
		,edw_customer_attr_hier_dim.aw_remote_key
		,edw_customer_attr_hier_dim.channel
		,edw_customer_attr_hier_dim.sls_grp
		,edw_customer_attr_hier_dim.store_typ
),
transformed as(
SELECT derived_table1.cntry
	,derived_table1.sold_to_party
	,derived_table1.channel
	,derived_table1.sls_grp
	,derived_table1.cust_hier_l1
	,derived_table1.cust_hier_l2
	,derived_table1.cust_hier_l3
	,derived_table1.cust_hier_l4
	,derived_table1.cust_hier_l5
	,derived_table1.store_typ AS store_type
FROM (
	select * from union1
	UNION ALL
	select * from union2
	
	) derived_table1
)
select * from transformed