
{% materialization incremental, default -%}

  {% set unique_key = config.get('unique_key') %}
  {% set full_refresh_mode = flags.FULL_REFRESH %}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {# -- set the type so our rename / drop uses the correct syntax #}
  {% set backup_type = existing_relation.type | default("table") %}
  {% set backup_relation = make_temp_relation(this, "__dbt_backup").incorporate(type=backup_type) %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}
  {% if existing_relation is none %}
      {% do run_query(create_table_as(False, target_relation, sql), "main") %}
  {% elif existing_relation.is_view or full_refresh_mode %}
      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% do run_query(create_table_as(False, target_relation, sql), "main") %}
      {% do to_drop.append(backup_relation) %}
  {% else %}
      {% set tmp_relation = make_temp_relation(target_relation) %}
      {% do run_query(create_table_as(True, tmp_relation, sql)) %}
      {% do adapter.expand_target_column_types(
             from_relation=tmp_relation,
             to_relation=target_relation) %}
      {% do incremental_upsert(tmp_relation, target_relation, unique_key=unique_key) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do drop_relation_if_exists(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

{%- endmaterialization %}
