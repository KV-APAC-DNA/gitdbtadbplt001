{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- if custom_alias_name and (node.schema.lower().endswith('workspace') or node.schema.lower().contains('dbt_cloud_pr') -%}
        {{ node.name }}
    {%- elif custom_alias_name -%}
        {{ custom_alias_name | trim }}
    {%- elif node.version -%}
        {{ return(node.name ~ "_v" ~ (node.version | replace(".", "_"))) }}
    {%- else -%}
        {{ node.name }}
    {%- endif -%}
{%- endmacro %}