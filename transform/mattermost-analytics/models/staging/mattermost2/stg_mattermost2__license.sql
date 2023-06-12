
WITH license AS (
    SELECT DISTINCT
       user_id     AS server_id
     , license_id  AS license_id
     , customer_id AS customer_id
     , NULL        AS installation_id
     , edition     AS edition
     , users       AS users
     , (to_timestamp(issued/1000)::DATE) AS issued_date
     , (to_timestamp(_start/1000)::DATE) AS start_date
     , (to_timestamp(expire/1000)::DATE) AS expire_date
    FROM
      {{ source('mattermost2', 'license') }}
)
SELECT *
FROM license