{% macro mcr_month(date) %} 
	formatDateTime({{date}}, 'FY%Y M%m') 
{% endmacro %}
