{% macro mcr_is_storage_host() %} 
	{% for host in var('storage_hosts') %}
		{% if target.host == host %}
			 {{ return (true)}}
		{% endif %}
	{% endfor %}
	{{ return (false)}}
{% endmacro %}
