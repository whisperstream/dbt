
{% macro run_query(sql, name="run_query_statement") %}
  {% call statement(name, fetch_result=true, auto_begin=false) %}
    {{ sql }}
  {% endcall %}

  {% do return(load_result(name).table) %}
{% endmacro %}
