with user_metrics as (
    select activity_date,
    server_id AS server_id,
    {{ dbt_utils.pivot(
      'user_role',
      dbt_utils.get_column_values(ref('int_user_nps_score_spined'), 'user_role'),
      agg='sum',
      then_value='promoters',
      quote_identifiers=False,
      prefix='count_',
      suffix='_promoters_daily'
  ) }},
    {{ dbt_utils.pivot(
      'user_role',
      dbt_utils.get_column_values(ref('int_user_nps_score_spined'), 'user_role'),
      agg='sum',
      then_value='detractors',
      quote_identifiers=False,
      prefix='count_',
      suffix='_detractors_daily'
  ) }},
    {{ dbt_utils.pivot(
      'user_role',
      dbt_utils.get_column_values(ref('int_user_nps_score_spined'), 'user_role'),
      agg='sum',
      then_value='passives',
      quote_identifiers=False,
      prefix='count_',
      suffix='_passives_daily'
  ) }},
    {{ dbt_utils.pivot(
      'user_role',
      dbt_utils.get_column_values(ref('int_user_nps_score_spined'), 'user_role'),
      agg='sum',
      then_value='nps_users',
      quote_identifiers=False,
      prefix='count_',
      suffix='_nps_users_daily'
  ) }},
    {{ dbt_utils.pivot(
      'user_role',
      dbt_utils.get_column_values(ref('int_user_nps_score_spined'), 'user_role'),
      agg='sum',
      then_value='promoters_last90d',
      quote_identifiers=False,
      prefix='count_',
      suffix='_promoters_last90d'
  ) }},
    {{ dbt_utils.pivot(
      'user_role',
      dbt_utils.get_column_values(ref('int_user_nps_score_spined'), 'user_role'),
      agg='sum',
      then_value='detractors_last90d',
      quote_identifiers=False,
      prefix='count_',
      suffix='_detractors_last90d'
  ) }},
    {{ dbt_utils.pivot(
      'user_role',
      dbt_utils.get_column_values(ref('int_user_nps_score_spined'), 'user_role'),
      agg='sum',
      then_value='passives_last90d',
      quote_identifiers=False,
      prefix='count_',
      suffix='_passives_last90d'
  ) }},
    {{ dbt_utils.pivot(
      'user_role',
      dbt_utils.get_column_values(ref('int_user_nps_score_spined'), 'user_role'),
      agg='sum',
      then_value='nps_users_last90d',
      quote_identifiers=False,
      prefix='count_',
      suffix='_nps_users_last90d'
  ) }}
    FROM
    {{ ref('int_user_nps_score_spined') }}
    group by activity_date
    , server_id
)
SELECT a.*,
    {{ dbt_utils.generate_surrogate_key(['activity_date', 'server_id']) }} AS daily_server_id
    b.server_version AS server_version,
    a.user_promoters + a.team_admin_promoters + a.system_admin_promoters AS count_promoters_daily,
    a.user_detractors + a.team_admin_detractors + a.system_admin_detractors AS count_detractors_daily,
    a.user_passives + a.team_admin_passives + a.system_admin_passives AS count_passives_daily,
    a.user_nps_users + a.team_admin_nps_users + a.system_admin_nps_users AS count_nps_users_daily,
    a.user_quarterly_promoters + a.team_admin_quarterly_promoters + a.system_admin_quarterly_promoters AS count_promoters_last90d,
    a.user_quarterly_detractors + a.team_admin_quarterly_detractors + a.system_admin_quarterly_detractors AS count_detractors_last90d,
    a.user_quarterly_passives + a.team_admin_quarterly_passives + a.system_admin_quarterly_passives AS count_passives_last90d,
    a.user_quarterly_nps_users + a.team_admin_quarterly_nps_users + a.system_admin_quarterly_nps_users AS count_nps_users_last90d
    from user_metrics a join
    {{ ref('int_nps_server_version_spined') }} b 
    on a.server_id = b.server_id and a.activity_date = b.activity_date
