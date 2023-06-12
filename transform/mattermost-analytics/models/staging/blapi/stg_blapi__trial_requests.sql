
WITH trial_requests AS (
    SELECT DISTINCT
      server_id     AS server_id
      , id          AS license_id
      , name        AS name
      , site_url    AS site_url
      , email       AS email
      , 'E20 Trial' AS edition
      , users       AS users
      , license_issued_at::date AS issued_date
      , start_date::date        AS start_date
      , end_date::date          AS expire_date
    FROM
      {{ source('blapi', 'trial_requests') }}
)
SELECT *
FROM trial_requests