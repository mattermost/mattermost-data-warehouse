{{config({
    "materialized": 'table',
    "schema": "mattermost"
  })
}}

with website_visits_w_adgroup as (
    select daily_website_traffic.anonymous_id, daily_website_traffic.timestamp, daily_website_traffic.context_page_path, daily_website_traffic.context_campaign_name, pageview_adgroup.campaignname
    from {{ ref('daily_website_traffic') }}
    left join {{ source('google_ads','ad_groups')}} as pageview_adgroup on daily_website_traffic.context_campaign_name = pageview_adgroup.CAMPAIGNNAME and daily_website_traffic.context_campaign_adgroup = pageview_adgroup.name
    where daily_website_traffic.context_campaign_name is not null
    group by 1, 2, 3, 4, 5
), forms_w_adgroup as (
    select marketo_forms.anonymous_id, marketo_forms.id as form_fill_id, marketo_forms.timestamp, marketo_forms.email, form_adgroup.campaignname, marketo_forms.context_page_path, marketo_forms.formid
    from {{ ref('marketo_forms') }}
    left join {{ source('google_ads','ad_groups')}} as form_adgroup on marketo_forms.obility_id__c = form_adgroup.id
), raw_form_adgroup_attribution as (
    select forms_w_adgroup.anonymous_id, forms_w_adgroup.form_fill_id, forms_w_adgroup.timestamp, forms_w_adgroup.EMAIL, forms_w_adgroup.context_page_path, forms_w_adgroup.formid, coalesce(forms_w_adgroup.campaignname, website_visits_w_adgroup.campaignname) as campaignname,
           ROW_NUMBER() OVER ( PARTITION BY forms_w_adgroup.anonymous_id ORDER BY  forms_w_adgroup.timestamp DESC) as rank
    from forms_w_adgroup
    left join website_visits_w_adgroup on forms_w_adgroup.anonymous_id = website_visits_w_adgroup.anonymous_id
        and forms_w_adgroup.timestamp > website_visits_w_adgroup.timestamp
        and website_visits_w_adgroup.campaignname is not null
), form_adgroup_attribution as (
    select raw_form_adgroup_attribution.*, marketo_form_names.name as form_name, formid as form_id
    from raw_form_adgroup_attribution
    left join {{ ref('marketo_form_names') }} on raw_form_adgroup_attribution.formid = marketo_form_names.form_id
    where raw_form_adgroup_attribution.campaignname is not null and rank = 1
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
), form_attribution as (
    select form_adgroup_attribution.*, coalesce(lead.status, case when contact.sfid is not null then 'Contact Only' else NULL end) as lead_status, lead.sfid as leadid, contact.sfid as contactid
    from form_adgroup_attribution
    left join {{ ref('lead') }} on lower(lead.email) = lower(form_adgroup_attribution.email)
    left join {{ ref('contact') }} on lower(contact.email) = lower(form_adgroup_attribution.email)
)

select * from form_attribution