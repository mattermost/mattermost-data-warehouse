{% macro create_parse_qs_udf() %}

create or replace function {{target.schema}}.parse_qs(value varchar)
returns object
language python
runtime_version = '3.10'
handler = 'parse_query_string'
as

$$
from urllib.parse import parse_qs

def parse_query_string(value):
    return { k: v[0] if v else None for k, v in parse_qs(value, keep_blank_values=True).items()}
$$
;

{% endmacro %}