{% macro create_extract_license_data_udf() %}

create or replace function {{target.schema}}.extract_license_data(value varchar)
returns object
language python
runtime_version = '3.10'
handler = 'extract_license_data'
as

$$
import json
from base64 import b64decode

def extract_license_data(value):
    decoded = b64decode(value)
    while ord('}') in decoded:
        try:
            return json.loads(decoded[0:decoded.rindex(ord('}')) + 1])
        except:
            decoded = decoded[0:decoded.rindex(ord('}'))]
$$
;

{% endmacro %}