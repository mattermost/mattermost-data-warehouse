
with source as (

    select * from {{ source('salesforce', 'account') }}

),

renamed as (

    select
          id as account_id

        -- Foreign Keys
        , ownerid as owner_id
        , parentid as parent_id
        , createdbyid as created_by_id
        , recordtypeid as record_type_id
        , lastmodifiedbyid as last_modified_by_id
        , masterrecordid as master_record_id

        -- Details
        , name as name
        , ownership as ownership
        , phone as phone
        , rating as rating
        , site as site
        , type as type
        , annualrevenue as annual_revenue
        , website as website
        , numberofemployees as number_of_employees
        , description as description
        , fax as fax
        , industry as industry
        , isdeleted as is_deleted
        , accountsource as account_source
        , sic as sic
        , sicdesc as sicdesc
        , tickersymbol as ticker_symbol

        -- Address Details
        , billingcity as billing_city
        , billingcountry as billing_country
        , billingcountrycode as billing_country_code
        , billinglatitude as billing_latitude
        , billinglongitude as billing_longitude
        , billingpostalcode as billing_postal_code
        , billingstate as billing_state
        , billingstatecode as billing_state_code
        , billingstreet as billing_street
        , shippingcity as shipping_city
        , shippingcountry as shipping_country
        , shippingcountrycode as shipping_country_code
        , shippingpostalcode as shipping_postal_code
        , shippingstate as shipping_state
        , shippingstatecode as shipping_state_code
        , shippingstreet as shipping_street


        --Metadata
        , createddate as created_at
        , lastactivitydate as last_activity_at
        , lastmodifieddate as last_modified_at
        , lastreferenceddate as last_referenced_at
        , lastvieweddate as last_viewed_at

        , systemmodstamp as system_modstamp_at


        --Custom Columns
        , cbit__clearbitdomain__c as cbit__clearbitdomain__c
        , clearbit_employee_count__c as clearbit_employee_count__c
        , company_type__c as company_type__c
        , cosize__c as cosize__c
        , csm_lookup__c as csm_lookup__c
        , csm_override__c as csm_override__c
        , csm_zd__c as csm_zd__c
        , customer_engineer_zd__c as customer_engineer_zd__c
        , customer_engineer__c as customer_engineer__c
        , customer_finance_status__c as customer_finance_status__c
        , customer_segmentation_tier__c as customer_segmentation_tier__c
        , date_of_issue__c as date_of_issue__c
        , demo_req_date__c as demo_req_date__c
        , discoverorg_employee_count__c as discoverorg_employee_count__c
        , dwh_external_id__c as dwh_external_id__c
        , employee_count_override__c as employee_count_override__c
        , engagio__numberofpeople__c as engagio__numberofpeople__c
        , e_purchase_date__c as e_purchase_date__c
        , first_created_date__c as first_created_date__c
        , first_offer__c as first_offer__c
        , followup_date__c as followup_date__c
        , geo__c as geo__c
        , getstarteddate__c as getstarteddate__c
        , government__c as government__c
        , health_score__c as health_score__c
        , imported_case_study_note__c as imported_case_study_note__c
        , imported_industry__c as imported_industry__c
        , inbound_outbound__c as inbound_outbound__c
        , lastreviewed__c as lastreviewed__c
        , latest_telemetry_date__c as latest_telemetry_date__c
        , lead_created_date__c as lead_created_date__c
        , legally_agreed_to_joint_pr__c as legally_agreed_to_joint_pr__c
        , legally_agreed_to_pr__c as legally_agreed_to_pr__c
        , legal_right_for_case_studies__c as legal_right_for_case_studies__c
        , max_closed_won_date__c as max_closed_won_date__c
        , meetingset_date__c as meetingset_date__c
        , named_account_tier__c as named_account_tier__c
        , named_account__c as named_account__c
        , nda_expiration_date__c as nda_expiration_date__c
        , netsuite_conn__celigo_update__c as netsuite_conn__celigo_update__c
        , netsuite_conn__credit_hold__c as netsuite_conn__credit_hold__c
        , netsuite_conn__netsuite_id__c as netsuite_conn__netsuite_id__c
        , netsuite_conn__netsuite_sync_err__c as netsuite_conn__netsuite_sync_err__c
        , netsuite_conn__pushed_from_opportunity__c as netsuite_conn__pushed_from_opportunity__c
        , netsuite_conn__push_to_netsuite__c as netsuite_conn__push_to_netsuite__c
        , netsuite_conn__sync_in_progress__c as netsuite_conn__sync_in_progress__c
        , offer_detail__c as offer_detail__c
        , offer__c as offer__c
        , other_customer_marketing_comment__c as other_customer_marketing_comment__c
        , our_champion__c as our_champion__c
        , parent_s_parent_acount__c as parent_s_parent_acount__c
        , region__c as region__c
        , request_a_quote_date__c as request_a_quote_date__c
        , responded_date__c as responded_date__c
        , seats_active_latest__c as seats_active_latest__c
        , seats_active_mau__c as seats_active_mau__c
        , seats_active_max__c as seats_active_max__c
        , seats_active_override__c as seats_active_override__c
        , seats_active_wau__c as seats_active_wau__c
        , seats_licensed__c as seats_licensed__c
        , seats_registered__c as seats_registered__c
        , seat_utilization__c as seat_utilization__c
        , sector__c as sector__c
        , server_version__c as server_version__c
        , signed_nda__c as signed_nda__c
        , SMB_MME__c as smb_mme__c
        , sponsor__c as sponsor__c
        , telemetry_accuracy__c as telemetry_accuracy__c
        , territoryid__c as territoryid__c
        , territory_area__c as territory_area__c
        , territory_comm_rep__c as territory_comm_rep__c
        , territory_ent_rep__c as territory_ent_rep__c
        , territory_geo__c as territory_geo__c
        , territory_last_updated__c as territory_last_updated__c
        , territory_region__c as territory_region__c
        , territory_segment__c as territory_segment__c
        , territory__c as territory__c
        , total_sales__c as total_sales__c
        , total_seats__c as total_seats__c
        , trial_req_date__c as trial_req_date__c
        , true_up_eligible__c as true_up_eligible__c
        , unqualified_date__c as unqualified_date__c
        , account_end_date__c as account_end_date__c
        , account_number_int__c as account_number_int__c
        , account_owner_zd__c as account_owner_zd__c
        , account_start_date__c as account_start_date__c
        , api_id__c as api_id__c
        , arr_current__c as arr_current__c
        , cleaned_up_website__c as cleaned_up_website__c
        , elasticsearch__c as elasticsearch__c
        , read_only_announcement_channels__c as read_only_announcement_channels__c
        , high_availability_ha__c as high_availability_ha__c
        , saml_sso__c as saml_sso__c
        , ldap_group_sync__c as ldap_group_sync__c
        , compliance_custom_data_retention__c as compliance_custom_data_retention__c
        , incident_collaboration__c as incident_collaboration__c
        , integrations__c as integrations__c
        , sector_picklist__c as sector_picklist__c

        -- Stitch columns omitted

    from source

)

select
    *
from
    renamed
where
    not is_deleted