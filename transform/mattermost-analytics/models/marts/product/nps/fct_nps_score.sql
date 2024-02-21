with user_metrics as (
    select activity_date,
    server_id AS server_id,
    {{ dbt_utils.pivot(
      'user_role',
      dbt_utils.get_column_values(ref('int_user_nps_score_spined'), 'user_role'),
      agg='sum',
      then_value='promoters',
      quote_identifiers=False,
      suffix='_promoters'
  ) }},
    {{ dbt_utils.pivot(
      'user_role',
      dbt_utils.get_column_values(ref('int_user_nps_score_spined'), 'user_role'),
      agg='sum',
      then_value='detractors',
      quote_identifiers=False,
      suffix='_detractors'
  ) }},
    {{ dbt_utils.pivot(
      'user_role',
      dbt_utils.get_column_values(ref('int_user_nps_score_spined'), 'user_role'),
      agg='sum',
      then_value='passives',
      quote_identifiers=False,
      suffix='_passives'
  ) }},
    {{ dbt_utils.pivot(
      'user_role',
      dbt_utils.get_column_values(ref('int_user_nps_score_spined'), 'user_role'),
      agg='sum',
      then_value='nps_users',
      quote_identifiers=False,
      suffix='_nps_users'
  ) }},
    {{ dbt_utils.pivot(
      'user_role',
      dbt_utils.get_column_values(ref('int_user_nps_score_spined'), 'user_role'),
      agg='sum',
      then_value='quarterly_promoters',
      quote_identifiers=False,
      suffix='_quarterly_promoters'
  ) }},
    {{ dbt_utils.pivot(
      'user_role',
      dbt_utils.get_column_values(ref('int_user_nps_score_spined'), 'user_role'),
      agg='sum',
      then_value='quarterly_detractors',
      quote_identifiers=False,
      suffix='_quarterly_detractors'
  ) }},
    {{ dbt_utils.pivot(
      'user_role',
      dbt_utils.get_column_values(ref('int_user_nps_score_spined'), 'user_role'),
      agg='sum',
      then_value='quarterly_passives',
      quote_identifiers=False,
      suffix='_quarterly_passives'
  ) }},
    {{ dbt_utils.pivot(
      'user_role',
      dbt_utils.get_column_values(ref('int_user_nps_score_spined'), 'user_role'),
      agg='sum',
      then_value='quarterly_nps_users',
      quote_identifiers=False,
      suffix='_quarterly_nps_users'
  ) }}
    FROM
    {{ ref('int_user_nps_score_spined') }}
    group by activity_date
    , server_id
)
SELECT a.*,
    b.server_version AS server_version,
    a.user_promoters + a.team_admin_promoters + a.system_admin_promoters AS promoters,
    a.user_detractors + a.team_admin_detractors + a.system_admin_detractors AS detractors,
    a.user_passives + a.team_admin_passives + a.system_admin_passives AS passives,
    a.user_nps_users + a.team_admin_nps_users + a.system_admin_nps_users AS nps_users,
    a.user_quarterly_promoters + a.team_admin_quarterly_promoters + a.system_admin_quarterly_promoters AS quarterly_promoters,
    a.user_quarterly_detractors + a.team_admin_quarterly_detractors + a.system_admin_quarterly_detractors AS quarterly_detractors,
    a.user_quarterly_passives + a.team_admin_quarterly_passives + a.system_admin_quarterly_passives AS quarterly_passives,
    a.user_quarterly_nps_users + a.team_admin_quarterly_nps_users + a.system_admin_quarterly_nps_users AS quarterly_nps_users
    from user_metrics a join
    {{ ref('int_nps_server_version_spined') }} b 
    on a.server_id = b.server_id and a.activity_date = b.activity_date
