-- we have it as separate macros becase with a time we wil add more transformaiton here not just domain funcyion
{% macro mcr_normalize_domain_id(website) %}
    lower(domainWithoutWWW( {{website}} ))
{% endmacro %}