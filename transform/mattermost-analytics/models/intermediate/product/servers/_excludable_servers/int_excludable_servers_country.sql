select
    server_id,
    'Country' as reason
from
    {{ ref('int_server_ip_to_country') }}
where
    last_known_ip_country in (
{% for country in var('excluded_countries') %}
    '{{ country }}'{% if not loop.last %},{% endif %}
{% endfor %}
    )