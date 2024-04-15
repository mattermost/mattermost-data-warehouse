-- Ensure that stripe and license data do not overlap.

select
    server_id, license_id, installation_id
from
    {{ ref('int_latest_server_customer_info')}}
where
    (license_company_name is not null and cloud_company_name is not null and license_company_name <> cloud_company_name)
    or (license_contact_email is not null and cloud_contact_email is not null and license_contact_email <> cloud_contact_email)
