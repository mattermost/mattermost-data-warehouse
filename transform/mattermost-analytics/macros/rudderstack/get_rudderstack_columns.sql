{% macro get_rudderstack_columns() %}

   {%-
        set rudderstack_columns = ['id', 'anonymous_id', 'received_at', 'sent_at', 'original_timestamp', 'timestamp', 
        'context_ip', 'event', 'event_text']
    -%}

    {% for column in rudderstack_columns %}
        {{ column }} AS {{ column }}
        {% if not loop.last %},{% endif %}
    {% endfor %}

{% endmacro %}
