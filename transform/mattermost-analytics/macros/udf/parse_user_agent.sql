{% macro create_parse_user_agent_udf() %}

create or replace function {{ target.database }}.{{ target.schema }}.parse_user_agent(useragent text)
returns variant
language python
runtime_version = '3.10'
packages = ('ua-parser')
handler = 'parse_user_agent'
as
$$
from ua_parser import user_agent_parser

def parse_user_agent(useragent):
    parsed_string = user_agent_parser.Parse(useragent)
    return {
        'browser_family': parsed_string.get('user_agent', {}).get('family'),
        'browser_version_major': parsed_string.get('user_agent', {}).get('major'),
        'browser_version_minor': parsed_string.get('user_agent', {}).get('minor'),
        'browser_version_patch': parsed_string.get('user_agent', {}).get('patch'),
        'os_family': parsed_string.get('os', {}).get('family'),
        'os_version_major': parsed_string.get('os', {}).get('major'),
        'os_version_minor': parsed_string.get('os', {}).get('minor'),
        'os_version_patch': parsed_string.get('os', {}).get('patch'),
        'os_version_patch_minor': parsed_string.get('os', {}).get('patch_minor'),
        'device_family': parsed_string.get('device', {}).get('family'),
        'device_brand': parsed_string.get('device', {}).get('brand'),
        'device_model': parsed_string.get('device', {}).get('model')
    }
    $$
;

{% endmacro %}