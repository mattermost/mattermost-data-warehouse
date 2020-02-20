with account_health_facts as (
    select account.name
         , account.sfid as account_sfid
         , max(end_date__c)::date                                                          as end_date
         , min(start_date__c)::date                                                        as start_date
         , max(end_date__c) > current_date                                                 as existing_customer
         , (least(max(end_date__c)::date, current_date) - min(start_date__c)::date) / 365  as tenure_in_yrs
         , count(distinct TICKETS.ID)                                                      as count_tickets
         , count(distinct CASE WHEN TICKETS.STATUS = 'open' THEN TICKETS.ID ELSE NULL END) as open_tickets
         ,count(distinct tasks_filtered.sfid)
    from orgm.account
             left join orgm.opportunity on opportunity.accountid = account.sfid and iswon
             left join orgm.opportunitylineitem on opportunitylineitem.opportunityid = opportunity.sfid
             left join orgm.tasks_filtered on account.sfid = tasks_filtered.accountid AND tasks_filtered.createddate > current_date - interval '120 days'
             left join ZENDESK_RAW.ORGANIZATIONS on ORGANIZATIONS.ID = ZENDESK__ZENDESK_ORGANIZATION_ID__C
             left join ZENDESK_RAW.TICKETS on TICKETS.ORGANIZATION_ID = ORGANIZATIONS.ID AND TICKETS.CREATED_AT > current_date - interval '90 days'
    where account.sfid in (
                           '00136000015TnjTAAS',
                           '0013600001Wp4HwAAJ',
                           '00136000011sQHJAA2',
                           '0013600001PTrLtAAL',
                           '0011R000020VC8UQAW'
        )
    group by 1, 2
)
select name,
       account_sfid,
       tenure_in_yrs,
       case
            when tenure_in_yrs <= 0.5 then (1 - .75)
            when tenure_in_yrs <= 1 then (1 - .50)
            when tenure_in_yrs <= 2 then (1 - .25)
            -- (1 - risk %)
       else (1 - .10) end
       * 25 as tenure_health_score,
       -- tenure_in_yrs 25% of their health score
       count_tickets,
       case
            when count_tickets >= 5 then (1 - .75)
            when count_tickets >= 3 then (1 - .50)
            when count_tickets >= 1 then (1 - .25)
            when count_tickets = 0 then (1 - .50)
            -- (1 - risk %)
       else (1 - .10) end
       * 25 as ticket_health_score,
       -- ticket 25% of their health score
       20 as cs_attrition_likelihood,
       tenure_health_score + ticket_health_score as og_health_score,
       greatest(tenure_health_score + ticket_health_score - cs_attrition_likelihood,0) as health_score
from account_health_facts