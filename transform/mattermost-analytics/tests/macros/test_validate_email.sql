with test_values as (
    select
        *
    from (
        values
            ('test@user.c o'),
            ('te\\st@test.com'),
            ('t(est@test.com'),
            ('t(es)t@test.com'),
            ('a&b@test.com'),
            ('$user@test.com'),
            ('a/b@test.com'),
            ('a!b@test.com'),
            ('<user@test.com>'),
            ('a“b@test.com'),
            ('ab@test.co\\u2006m'),
            ('ab@test.co\\uu00a0'),
            ('ab@test.com`')
    ) as t(test_email)
)
select
    {{ validate_email('test_email') }} as is_valid_email
from
    test_values
where
    -- No rows should be reported as valid
    is_valid_email