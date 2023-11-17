{% macro mcr_quarter(date) %} 
	formatDateTime({{date}}, 'FY%Y Q%Q') 
{% endmacro %}
