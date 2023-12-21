{{ codegen.generate_source(
        schema_name= 'ASPSDL_RAW', 
        database_name= 'DEV_DNA_LOAD',
        generate_columns=true,
        table_names= [
        'SDL_CODE_DESCRIPTIONS'
        ]
) }}