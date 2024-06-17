with source as
(
    select * from {{ref('idnedw_integration__edw_rpt_id_sellin_analysis')}}
)

select
	bill_dt as "bill_dt",
	bill_doc as "bill_doc",
	jj_year as "jj_year",
	jj_qrtr as "jj_qrtr",
	jj_mnth_id as "jj_mnth_id",
	jj_mnth as "jj_mnth",
	jj_mnth_desc as "jj_mnth_desc",
	jj_mnth_no as "jj_mnth_no",
	jj_mnth_long as "jj_mnth_long",
	dstrbtr_grp_cd as "dstrbtr_grp_cd",
	dstrbtr_grp_nm as "dstrbtr_grp_nm",
	jj_sap_dstrbtr_id as "jj_sap_dstrbtr_id",
	jj_sap_dstrbtr_nm as "jj_sap_dstrbtr_nm",
	dstrbtr_cd_nm as "dstrbtr_cd_nm",
	area as "area",
	region as "region",
	bdm_nm as "bdm_nm",
	rbm_nm as "rbm_nm",
	dstrbtr_status as "dstrbtr_status",
	jj_sap_prod_id as "jj_sap_prod_id",
	jj_sap_prod_desc as "jj_sap_prod_desc",
	jj_sap_upgrd_prod_id as "jj_sap_upgrd_prod_id",
	jj_sap_upgrd_prod_desc as "jj_sap_upgrd_prod_desc",
	jj_sap_cd_mp_prod_id as "jj_sap_cd_mp_prod_id",
	jj_sap_cd_mp_prod_desc as "jj_sap_cd_mp_prod_desc",
	sap_prod_code_name as "sap_prod_code_name",
	franchise as "franchise",
	brand as "brand",
	sku_grp_or_variant as "sku_grp_or_variant",
	sku_grp1_or_variant1 as "sku_grp1_or_variant1",
	sku_grp2_or_variant2 as "sku_grp2_or_variant2",
	sku_grp3_or_variant3 as "sku_grp3_or_variant3",
	prod_status as "prod_status",
	sellin_qty as "sellin_qty",
	sellin_val as "sellin_val",
	gross_sellin_val as "gross_sellin_val"
from source