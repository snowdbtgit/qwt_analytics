{% macro grant_select(role) %}
{% set sql %}
    grant usage on warehouse loading_wh to role {{ role }};
    grant usage on database qwt_analytics to role {{ role }};
    grant usage on schema qwt_analytics.staging to role {{ role }};
    grant select on all tables in schema qwt_analytics.staging to role {{ role }};
    grant select on all views in schema qwt_analytics.staging to role {{ role }};
{% endset %}
 
{% do run_query(sql) %}
{% do log("Privileges granted", info=True) %}
{% endmacro %}