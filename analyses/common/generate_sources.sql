{{ codegen.generate_source(
        schema_name= 'chnsdl_raw', 
        database_name= 'DEV_DNA_LOAD',
        generate_columns=true,
        table_names= [
        'SDL_MDS_CN_ECOM_PRODUCT',
        'SDL_MDS_CN_SKU_BENCHMARKS'
        ]
) }}