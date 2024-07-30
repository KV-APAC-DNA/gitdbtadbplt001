
with item_m_stg as(
    select * from {{ ref('jpnwks_integration__item_m_stg') }}
),

edi_brand_m as (
    select * from {{ ref('jpnedw_integration__edi_brand_m') }}
),

wk_so_planet_no_dup as (
    select * from {{ ref('jpnwks_integration__wk_so_planet_no_dup') }}
),

edi_jedpar as (
    select * from {{ ref('jpnedw_integration__edi_jedpar') }}
),

edi_item_m as (
    select * from {{ ref('jpnedw_integration__edi_item_m') }}
)
,

t1 as (
            select ta.jcp_rec_seq,
                    ta.ws_cd,
                    tb.v_item_cd_jc
                from (
                    select a.*
                    from wk_so_planet_no_dup a
                    ) ta,
                    item_m_stg tb
                where ta.jcp_rec_seq = tb.jcp_rec_seq
),

t2 as (
            select distinct a.jan_cd,
                        a.jan_cd_so,
                        a.mega_brnd_chkflg,
                        a.sap_cstm_type,
                        a.mega_brnd
                    from edi_item_m a
),

t3 as (
                select distinct mega_brand,
                        sap_cstm_type
                    from edi_brand_m
)

,
tab as (
            select t1.jcp_rec_seq,
                t1.ws_cd,
                case 
                    when t2.mega_brnd_chkflg = 1
                        and t2.sap_cstm_type is not null
                        then t2.sap_cstm_type
                    else case 
                            when t2.mega_brnd_chkflg <> 1
                                or t2.sap_cstm_type is null
                                then t3.sap_cstm_type
                            else 'DUMMY'
                            end
                    end as v_sap_cstm_type
            from t1,
                 t2,
                 t3
            where t2.jan_cd_so = t1.v_item_cd_jc
                and t3.mega_brand = t2.mega_brnd
)

,
cust as (
    select tab1.jcp_rec_seq,
            case 
                when tab1.v_sap_cstm_type = 'DUMMY'
                    then ' '
                else ltrim(tab2.customer_number, '0')
                end cust_cd,
            ltrim(tab2.int_partner_number, '0') sap_ship_to
        from tab tab1,
            edi_jedpar tab2
        where tab2.partner_customer_cd(+) = tab1.ws_cd
            and tab2.sap_cstm_type(+) = tab1.v_sap_cstm_type
            and tab2.van_type = 'PLANET'
            and tab2.partner_fn = 'WE'
),

result as 

(
    select distinct fnl.jcp_rec_seq,
            cust.cust_cd
    from cust,
        wk_so_planet_no_dup fnl
    where cust.jcp_rec_seq = fnl.jcp_rec_seq
),

final as
(
select 
    jcp_rec_seq::number(10,0) as jcp_rec_seq,
 	cust_cd::varchar(256) as cust_cd
from result
)

select * from final