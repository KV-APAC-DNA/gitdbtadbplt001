with 

source as (
    select * from {{ ref('aspitg_integration__itg_sap_bw_dna_material_bomlist') }}
),

final as (
    select
        zbomno :: varchar(10) as bom,
        plant :: varchar(6) as plant,
        material :: varchar(30) as matl_num,
        component :: varchar(30) as component,
        createdon as createdon,
        validfrom as validfrom,
        validto as validto,
        zvlfromc as validfrom_zvlfromc,
        zvltoi as validto_zvltoi,
        id_credat as createdon_id_credat,
        zcoqty as quantity,
        comp_scrap as componentscrap,
        base_uom :: varchar(50) as componentunit,
        recordmode :: varchar(50) as recordmode,
        zhdqty as basequantity,
        unit :: varchar(50) as baseunit,
        matl_type :: varchar(50) as materialtype,
        zmat_type :: varchar(50) as zmaterialtype,
        salesorg :: varchar(50) as sls_org,
        zstlan :: varchar(50) as bomusage,
        zstlal :: varchar(50) as alternativebom,
        zstlst as bomstatus,
        zz_tp :: varchar(50) as componentscrap_zztp,
        zz_cp :: varchar(50) as componentscrap_zzcp,
        zfmnge :: varchar(50) as fixedquantity,
        zausch :: varchar(50) as componentscrap_zausch,
        zuposz :: varchar(50) as subitemnumber,
        zmaktx :: varchar(50) as matl_desc,
        zupmng :: varchar(50) as subitemquantity,
        zlosbs :: varchar(50) as tolotsize,
        zlosvn :: varchar(50) as fromlotsize,
        znlfzt :: varchar(50) as leadtimeoffset,
        zverti :: varchar(50) as distribution,
        znlfzv :: varchar(50) as operationleadtimeoffset,
        znlfmv :: varchar(50) as operationleadtimeoffsetunit,
        zzausch :: varchar(50) as componentscrap_zzausch,
        zavoau :: varchar(50) as operationscrap,
        znlfzv1 :: varchar(50) as operationltoffset,
        crt_dttm as crt_dttm,
        updt_dttm as updt_dttm
    from source
)

select * from final