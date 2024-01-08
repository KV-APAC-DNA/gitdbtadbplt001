with 

source as (
    select * from {{ ref('aspitg_integration__itg_sap_bw_dna_material_bomlist') }}
),

final as (
    select
        zbomno as bom,
        plant as plant,
        material as matl_num,
        component as component,
        createdon as createdon,
        validfrom as validfrom,
        validto as validto,
        zvlfromc as validfrom_zvlfromc,
        zvltoi as validto_zvltoi,
        id_credat as createdon_id_credat,
        zcoqty as quantity,
        comp_scrap as componentscrap,
        base_uom as componentunit,
        recordmode as recordmode,
        zhdqty as basequantity,
        unit as baseunit,
        matl_type as materialtype,
        zmat_type as zmaterialtype,
        salesorg as sls_org,
        zstlan as bomusage,
        zstlal as alternativebom,
        zstlst as bomstatus,
        zz_tp as componentscrap_zztp,
        zz_cp as componentscrap_zzcp,
        zfmnge as fixedquantity,
        zausch as componentscrap_zausch,
        zuposz as subitemnumber,
        zmaktx as matl_desc,
        zupmng as subitemquantity,
        zlosbs as tolotsize,
        zlosvn as fromlotsize,
        znlfzt as leadtimeoffset,
        zverti as distribution,
        znlfzv as operationleadtimeoffset,
        znlfmv as operationleadtimeoffsetunit,
        zzausch as componentscrap_zzausch,
        zavoau as operationscrap,
        znlfzv1 as operationltoffset,
        crt_dttm as crt_dttm,
        updt_dttm as updt_dttm
    from source
)

select * from final