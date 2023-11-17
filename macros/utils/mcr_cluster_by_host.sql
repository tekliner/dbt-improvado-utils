{% macro mcr_cluster_by_host() %}
  {% if 'eu.improvado.io' in target.host %}
    {{ return('Tokyo') }}
  {% elif 'us.improvado.io' in target.host %}
    {{ return('Montana') }}
  {% else %}
    {{ return('Lisbon') }}
  {% endif %}
{% endmacro %}
