-- Query for the KPIs Overall: MQL/Signups/Demo Requests/Visits/Activated/Newlogos/PayingCustomers/MRR/ASP

-- MQL, Signups, and Demo Requests
with MQL_Signups_Demo as (select 
date_trunc('week', af.lead_mql_date)::date as week_date
, sum(case when af.lead_mql_date is not null and af.mql_flag = 1 then 1 else 0 end) as mqls
, sum(case when af.klaviyo_account_created_date is not null then 1 else 0 end) as signups
, sum(case when af.demo_request_date is not null then 1 else 0 end) as demo_requests                                                                          
from klaviyo.sandbox.tbl_acquisition_funnel af
where af.lead_mql_date >= '2022-01-01'
group by 1
order by 1 desc),
 --unique net new website visitors (marketing/site traffic only exclude in-app traffic) using aggregate heap tables
Visits as (
select 
date_trunc('week', visit_date)::date as week_date,
count(distinct user_id) as visits
from klaviyo.public.bi_website_visitors 
where visitor_type = 'Visitor'
and visit_type = 'Net New Visitor'
and page_classification = 'marketing site'
and visit_date >= '2022-01-01'
group by 1
order by 1 desc),
-- activated actions combined
--SS Activated (Signups that perform one of three actions (form, flow, campaign) - earliest time)    
activated_base as (
-- publish flow
  select current_company_id as aid, convert_timezone('America/Los_Angeles', 'UTC', min(time)) as first
  from heap_main_production.heap.set_flow_email_to_live
  group by 1

union
-- publish form
  select current_company_id as aid, convert_timezone('America/Los_Angeles', 'UTC', min(time)) as first
  from heap_main_production.heap.published_form
  group by 1
  
union
-- send or schedule campaign 
  select current_company_id as aid, convert_timezone('America/Los_Angeles', 'UTC', min(time)) as first
  from heap_main_production.heap.scheduled_campaign
  group by 1
  
), activated as (
-- earliest activated date by klaviyo account id    
    select aid as klaviyo_account_id
    , min(date_trunc('day', first)) as min_activated_date
    from activated_base 
    group by 1
    order by 1
    
), base_join as (
-- join bi_lead_id_mapping for dedup        
    select distinct 
        bil.* 
    , activated.min_activated_date       
    from klaviyo.sandbox.bi_lead_id_mapping bil    
     left join activated on activated.klaviyo_account_id = bil.klaviyo_account_id   
    
), lead_aggregation as (    
-- aggregate to bi_lead_id level (alrdy confirmed min('2021-01-02', null)='2021-01-02')
    select b.bi_lead_id
    , min(min_activated_date) as min_activated_date   
    from base_join b
    group by b.bi_lead_id
), Activated_Final as (
select 
date_trunc('week', la.min_activated_date)::date as earliest_activated_date
, count(distinct af.klaviyo_account_id) as activated
from klaviyo.sandbox.tbl_acquisition_funnel af
join lead_aggregation la on af.bi_lead_id = la.bi_lead_id
where earliest_activated_date >= '2022-01-01'
--Email
and af.lead_type in ('Email', 'Both')
group by 1
order by 1 desc),
Logos as (
select
date_trunc('week', MOST_RECENT_ACQUISITION_DATE)::date as start_date
,SUM(LOGO_NEW_COUNT) as new_Logos
from klaviyo.public.static_subscriptions
where SELF_SERVE_STATUS = 'Self-Serve' and MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
group by 1
order by 1 desc),
MRR_ASP as (--SS MRR_ASP
select
-- according to Evan Cover    
date_trunc('week', MOST_RECENT_ACQUISITION_DATE) as start_date
 --email asp numerator
,  sum(email_new_mrr) + sum(email_reactivation_mrr) + sum(email_additional_product_mrr) as mrr
 --email asp denominator
, sum(email_logo_new_count)+ sum(email_logo_reactivation_count) + sum(email_logo_additional_product_count) as paying_customer_cnt
, round(div0(MRR, paying_customer_cnt),2) as asp   
from klaviyo.public.static_subscriptions
where MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
group by 1
order by 1 desc)

select M.week_date, 
        M.mqls, 
        M.signups, 
        M.demo_requests, 
        round(div0(M.mqls,V.visits),4)*100 as MQL_CVR,
        round(div0(M.signups,V.visits),4)*100 as Signup_CVR,
        round(div0(M.demo_requests,V.visits),4)*100 as DemoRequests_CVR,
        V.visits, 
        A.activated, 
        L.new_Logos,
        P.paying_customer_cnt,
        P.mrr,
        P.asp
from MQL_Signups_Demo M
left join Visits V on M.week_date = V.week_date
left join Activated_Final A on A.earliest_activated_date = M.week_date
left join Logos L on L.start_date = M.week_date
left join MRR_ASP P on P.start_date = M.week_date;



