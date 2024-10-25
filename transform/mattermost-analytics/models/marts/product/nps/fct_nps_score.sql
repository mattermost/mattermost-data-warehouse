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
    {{ dbt_utils.generate_surrogate_key(['a.server_id', 'a.activity_date']) }} as daily_server_id,
    {{ dbt_utils.generate_surrogate_key(['b.server_version_full']) }} AS version_id,
    a.count_user_promoters_daily + a.count_team_admin_promoters_daily + a.count_system_admin_promoters_daily AS count_promoters_daily,
    a.count_user_detractors_daily + a.count_team_admin_detractors_daily + a.count_system_admin_detractors_daily AS count_detractors_daily,
    a.count_user_passives_daily + a.count_team_admin_passives_daily + a.count_system_admin_passives_daily AS count_passives_daily,
    a.count_user_nps_users_daily + a.count_team_admin_nps_users_daily + a.count_system_admin_nps_users_daily AS count_nps_users_daily,
    a.count_user_promoters_last90d + a.count_team_admin_promoters_last90d + a.count_system_admin_promoters_last90d AS count_promoters_last90d,
    a.count_user_detractors_last90d + a.count_team_admin_detractors_last90d + a.count_system_admin_detractors_last90d AS count_detractors_last90d,
    a.count_user_passives_last90d + a.count_team_admin_passives_last90d + a.count_system_admin_passives_last90d AS count_passives_last90d,
    a.count_user_nps_users_last90d + a.count_team_admin_nps_users_last90d + a.count_system_admin_nps_users_last90d AS count_nps_users_last90d
    from user_metrics a join
    {{ ref('int_nps_server_version_spined') }} b 
    on a.server_id = b.server_id and a.activity_date = b.activity_date
