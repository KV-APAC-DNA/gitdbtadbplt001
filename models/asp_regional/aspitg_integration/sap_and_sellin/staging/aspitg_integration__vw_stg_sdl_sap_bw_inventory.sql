

--import cte
with bwa_inventory as(
    select * from {{ source('bwa_access', 'bwa_inventory') }}
),
itg_crncy_mult as(
    select * from {{ source('aspitg_integration', 'itg_crncy_mult') }}
),
--logical cte
final as(
    select 
        request as request_number,
        data_packet as data_packet,
        data_record as data_record,
        loc_currcy as loc_currcy,
        base_uom as base_uom,
        material as material,
        stor_loc as stor_loc,
        stocktype as stocktype,
        stockcat as stockcat,
        comp_code as comp_code,
        mat_plant as mat_plant,
        bic_zbatch as zbatch,
        plant as plant,
        whse_num as whse_num,
        strge_bin as strge_bin,
        strge_type as strge_type,
        indspecstk as indspecstk,
        bic_zmdsobkz as zmdsobkz,
        doc_date as doc_date,
        val_class as val_class,
        id_valarea as id_valarea,
        val_type as val_type,
        pstng_date as pstng_date,
        calday as calday,
        bic_zmat_wh as zmat_wh,
        version as version,
        vtype as vtype,
        vendor as vendor,
        sold_to as sold_to,
        bic_zmoveinds as zmoveinds,
        currency as currency,
        fiscvarnt as fiscvarnt,
        fiscyear as fiscyear,
        calyear as calyear,
        calmonth2 as calmonth2,
        calquart1 as calquart1,
        calquarter as calquarter,
        calweek as calweek,
        fiscper3 as fiscper3,
        halfyear1 as halfyear1,
        weekday1 as weekday1,
        fiscper as fiscper,
        calmonth as calmonth,
        case when curr1.currkey is null 
        then iff(right(recvs_val,1)='-',concat('-',replace(recvs_val,'-','')),recvs_val) 
        else iff(right(recvs_val,1)='-',concat('-',replace(recvs_val,'-','')),recvs_val) * pow(10,(2-curr1.currdec)) 
        end as recvs_val,
        case when curr1.currkey is null 
        then iff(right(issvs_val,1)='-',concat('-',replace(issvs_val,'-','')),issvs_val) 
        else iff(right(issvs_val,1)='-',concat('-',replace(issvs_val,'-','')),issvs_val) * pow(10,(2-curr1.currdec)) 
        end as issvs_val,
        iff(right(issblostck,1)='-',concat('-',replace(issblostck,'-','')),issblostck) as issblostck,
        iff(right(isscnsstck,1)='-',concat('-',replace(isscnsstck,'-','')),isscnsstck) as isscnsstck,
        iff(right(issqmstck,1)='-',concat('-',replace(issqmstck,'-','')),issqmstck) as issqmstck,
        iff(right(isstransst,1)='-',concat('-',replace(isstransst,'-','')),isstransst) as isstransst,
        iff(right(recblostck,1)='-',concat('-',replace(recblostck,'-','')),recblostck) as recblostck,
        iff(right(reccnsstck,1)='-',concat('-',replace(reccnsstck,'-','')),reccnsstck) as reccnsstck,
        iff(right(recqmstck,1)='-',concat('-',replace(recqmstck,'-','')),recqmstck) as recqmstck,
        iff(right(rectransst,1)='-',concat('-',replace(rectransst,'-','')),rectransst) as rectransst,
        iff(right(issscrp,1)='-',concat('-',replace(issscrp,'-','')),issscrp) as issscrp,
        case when curr1.currkey is null 
        then iff(right(issvalscrp,1)='-',concat('-',replace(issvalscrp,'-','')),issvalscrp) 
        else iff(right(issvalscrp,1)='-',concat('-',replace(issvalscrp,'-','')),issvalscrp) * pow(10,(2-curr1.currdec)) 
        end as issvalscrp,
        iff(right(rectotstck,1)='-',concat('-',replace(rectotstck,'-','')),rectotstck) as rectotstck,
        iff(right(isstotstck,1)='-',concat('-',replace(isstotstck,'-','')),isstotstck) as isstotstck,
        iff(right(issvalstck,1)='-',concat('-',replace(issvalstck,'-','')),issvalstck) as issvalstck,
        iff(right(recvalstck,1)='-',concat('-',replace(recvalstck,'-','')),recvalstck) as recvalstck,
        case when curr1.currkey is null 
        then iff(right(venconcon,1)='-',concat('-',replace(venconcon,'-','')),venconcon) 
        else iff(right(venconcon,1)='-',concat('-',replace(venconcon,'-','')),venconcon) * pow(10,(2-curr1.currdec)) 
        end as venconcon,
        case when curr1.currkey is null 
        then iff(right(recvalqm,1)='-',concat('-',replace(recvalqm,'-','')),recvalqm) 
        else iff(right(recvalqm,1)='-',concat('-',replace(recvalqm,'-','')),recvalqm) * pow(10,(2-curr1.currdec)) 
        end as recvalqm,
        case when curr1.currkey is null 
        then iff(right(recvalblo,1)='-',concat('-',replace(recvalblo,'-','')),recvalblo) 
        else iff(right(recvalblo,1)='-',concat('-',replace(recvalblo,'-','')),recvalblo) * pow(10,(2-curr1.currdec)) 
        end as recvalblo,
        case when curr1.currkey is null 
        then iff(right(issvalbloc,1)='-',concat('-',replace(issvalbloc,'-','')),issvalbloc) 
        else iff(right(issvalbloc,1)='-',concat('-',replace(issvalbloc,'-','')),issvalbloc) * pow(10,(2-curr1.currdec)) 
        end as issvalbloc,
        case when curr1.currkey is null 
        then iff(right(issvalqm,1)='-',concat('-',replace(issvalqm,'-','')),issvalqm) 
        else iff(right(issvalqm,1)='-',concat('-',replace(issvalqm,'-','')),issvalqm) * pow(10,(2-curr1.currdec)) 
        end as issvalqm,
        case when curr1.currkey is null 
        then iff(right(rectrfstva,1)='-',concat('-',replace(rectrfstva,'-','')),rectrfstva) 
        else iff(right(rectrfstva,1)='-',concat('-',replace(rectrfstva,'-','')),rectrfstva) * pow(10,(2-curr1.currdec)) 
        end as rectrfstva,
        case when curr1.currkey is null 
        then iff(right(isstrfstva,1)='-',concat('-',replace(isstrfstva,'-','')),isstrfstva) 
        else iff(right(isstrfstva,1)='-',concat('-',replace(isstrfstva,'-','')),isstrfstva) * pow(10,(2-curr1.currdec)) 
        end as isstrfstva,
        case when curr1.currkey is null 
        then iff(right(issccnsval,1)='-',concat('-',replace(issccnsval,'-','')),issccnsval) 
        else iff(right(issccnsval,1)='-',concat('-',replace(issccnsval,'-','')),issccnsval) * pow(10,(2-curr1.currdec)) 
        end as issccnsval,
        case when curr1.currkey is null 
        then iff(right(recccnsval,1)='-',concat('-',replace(recccnsval,'-','')),recccnsval) 
        else iff(right(recccnsval,1)='-',concat('-',replace(recccnsval,'-','')),recccnsval) * pow(10,(2-curr1.currdec)) 
        end as recccnsval,
        case when curr1.currkey is null 
        then iff(right(cppvlc,1)='-',concat('-',replace(cppvlc,'-','')),cppvlc) 
        else iff(right(cppvlc,1)='-',concat('-',replace(cppvlc,'-','')),cppvlc) * pow(10,(2-curr1.currdec)) 
        end as cppvlc,
        iff(right(cpquabu,1)='-',concat('-',replace(cpquabu,'-','')),cpquabu) as cpquabu,
        case when curr1.currkey is null 
        then iff(right(bic_zmddelct,1)='-',concat('-',replace(bic_zmddelct,'-','')),bic_zmddelct) 
        else iff(right(bic_zmddelct,1)='-',concat('-',replace(bic_zmddelct,'-','')),bic_zmddelct) * pow(10,(2-curr1.currdec)) 
        end as zmddelct,
        case when curr1.currkey is null 
        then iff(right(g_avv040,1)='-',concat('-',replace(g_avv040,'-','')),g_avv040) 
        else iff(right(g_avv040,1)='-',concat('-',replace(g_avv040,'-','')),g_avv040) * pow(10,(2-curr1.currdec)) 
        end as g_avv040,
        case when curr1.currkey is null 
        then iff(right(price_unit,1)='-',concat('-',replace(price_unit,'-','')),price_unit) 
        else iff(right(price_unit,1)='-',concat('-',replace(price_unit,'-','')),price_unit) * pow(10,(2-curr1.currdec)) 
        end as price_unit,
        bic_zlincount as zlincount,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from bwa_inventory src
	left join itg_crncy_mult curr1
	on src.loc_currcy = curr1.currkey
    where loc_currcy != 'LOC_CURRCY'
)
--final select
select * from final
