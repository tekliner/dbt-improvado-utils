{% macro mcr_is_palantir_host() %} 
	{% for host in var('palantir_hosts') %}
		{% if target.host == host %}
			 {{ return (true)}}
		{% endif %}
	{% endfor %}
	{{ return (false)}}
{% endmacro %}