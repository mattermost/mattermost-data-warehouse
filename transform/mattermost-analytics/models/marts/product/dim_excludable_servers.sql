select
  server_id,
  {{ dbt_utils.pivot(
      'reason',
      dbt_utils.get_column_values(ref('int_excludable_servers'), 'reason'),
      prefix='reason_'
  ) }}
from {{ ref('int_excludable_servers') }}
where server_id is not null
group by server_id