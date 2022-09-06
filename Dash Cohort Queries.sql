------- Combination: Overall / URL Brkdwn / Channel Brkdwn / 

-------Geo Info CTE --------------
with country_ref as (
select 
country
,geo
from klaviyo.sandbox.country_mapping
group by 1,2
)

SELECT *
FROM
(
    --------------------------Overall Summary---------------------------
    SELECT
    date_trunc('week',h.visit_start_time)::date as WEEK
    , 'Views' as Levels
    ,'Overall' as Brkdwn
    ,count(DISTINCT h.full_visitor_id) as visits
    ,COUNT(DISTINCT (CASE WHEN 1=1
            and h__value['eventInfo']['eventLabel'] = 'complete' 
            and h__value['eventInfo']['eventCategory'] = 'form'
            -- and regexp_like (h__value['eventInfo']['eventAction'], '(.*signup.*|.*requestDemo.*|.*demo request.*)', 'i')            
            and h__value['eventInfo']['eventAction'] in ('signup','requestDemo')
           THEN h.full_visitor_id END)) as Complete_Setup
    ,count(distinct case when af.lead_mql_date is not null and af.mql_flag = 1 and af.klaviyo_account_id is not null then af.klaviyo_account_id else null end) as mqls
    ,count(distinct case when af.klaviyo_account_created_date is not null and af.klaviyo_account_id is not null then af.klaviyo_account_id else null end) as signups
    ,count(distinct case when af.demo_request_date is not null and af.klaviyo_account_id is not null then af.klaviyo_account_id else null end) as demo_requests
    , round(div0(mqls, visits),4)*100 as MQL_CVR
    , round(div0(signups,visits),4)*100 as Signup_CVR
    , round(div0(demo_requests,visits),4)*100 as DemoRequests_CVR
    FROM staging.stg_ga_hits h
    LEFT JOIN public.TBL_GA_SESSIONS ga on h.full_visitor_id = ga.full_visitor_id and h.visit_id = ga.visit_id
    LEFT JOIN public.tbl_account_dimensions ad on ga.klaviyo_account_id = ad.klaviyo_account_id
    LEFT JOIN klaviyo.sandbox.tbl_acquisition_funnel af on ga.klaviyo_account_id = af.klaviyo_account_id
    where split_part(h.h__value:page.pagePath::string,'?',0)
    in (
    '/',
    '/features',
    '/features/ecommerce-marketing-automation',
    '/pricing',
    '/email-marketing',
    '/switch/from-mailchimp',
    '/ecommerce-integrations/woocommerce',
    '/switch',
    '/sms-marketing',
    '/features/templates') 
    and h.visit_start_time >= '2022-01-03' 
    and array_contains('New Visitor'::variant, ga_segment)
    GROUP BY 1,2,3

    union all

    --------------------------URL BRKDWN---------------------------
    SELECT
    date_trunc('week',h.visit_start_time)::date as WEEK
    , 'URL' as Levels
    ,split_part(h.h__value:page.pagePath::string,'?',0)as Brkdwn
    ,count(DISTINCT h.full_visitor_id) as visits
    ,COUNT(DISTINCT (CASE WHEN 1=1
            and h__value['eventInfo']['eventLabel'] = 'complete' 
            and h__value['eventInfo']['eventCategory'] = 'form'
            -- and regexp_like (h__value['eventInfo']['eventAction'], '(.*signup.*|.*requestDemo.*|.*demo request.*)', 'i')            
            and h__value['eventInfo']['eventAction'] in ('signup','requestDemo')
           THEN h.full_visitor_id END)) as Complete_Setup
    ,count(distinct case when af.lead_mql_date is not null and af.mql_flag = 1 and af.klaviyo_account_id is not null then af.klaviyo_account_id else null end) as mqls
    ,count(distinct case when af.klaviyo_account_created_date is not null and af.klaviyo_account_id is not null then af.klaviyo_account_id else null end) as signups
    ,count(distinct case when af.demo_request_date is not null and af.klaviyo_account_id is not null then af.klaviyo_account_id else null end) as demo_requests
    , round(div0(mqls, visits),4)*100 as MQL_CVR
    , round(div0(signups,visits),4)*100 as Signup_CVR
    , round(div0(demo_requests,visits),4)*100 as DemoRequests_CVR
    FROM staging.stg_ga_hits h
    LEFT JOIN public.TBL_GA_SESSIONS ga on h.full_visitor_id = ga.full_visitor_id and h.visit_id = ga.visit_id
    LEFT JOIN public.tbl_account_dimensions ad on ga.klaviyo_account_id = ad.klaviyo_account_id
    LEFT JOIN klaviyo.sandbox.tbl_acquisition_funnel af on ga.klaviyo_account_id = af.klaviyo_account_id
    where split_part(h.h__value:page.pagePath::string,'?',0)
    in (
    '/',
    '/features',
    '/features/ecommerce-marketing-automation',
    '/pricing',
    '/email-marketing',
    '/switch/from-mailchimp',
    '/ecommerce-integrations/woocommerce',
    '/switch',
    '/sms-marketing',
    '/features/templates') 
    and h.visit_start_time >= '2022-01-03' 
    and array_contains('New Visitor'::variant, ga_segment)
    GROUP BY 1,2,3

    union all 

    --------------------------Channel BRKDWN---------------------------
    SELECT
    date_trunc('week',h.visit_start_time)::date as WEEK
    , 'Channel' as LEVELs
    ,CUSTOM_CHANNEL_GROUPING_1 as Brkdwn
    ,count(DISTINCT h.full_visitor_id) as visits
    ,COUNT(DISTINCT (CASE WHEN 1=1
            and h__value['eventInfo']['eventLabel'] = 'complete' 
            and h__value['eventInfo']['eventCategory'] = 'form'
            -- and regexp_like (h__value['eventInfo']['eventAction'], '(.*signup.*|.*requestDemo.*|.*demo request.*)', 'i')            
            and h__value['eventInfo']['eventAction'] in ('signup','requestDemo')
           THEN h.full_visitor_id END)) as Complete_Setup
    ,count(distinct case when af.lead_mql_date is not null and af.mql_flag = 1 and af.klaviyo_account_id is not null then af.klaviyo_account_id else null end) as mqls
    ,count(distinct case when af.klaviyo_account_created_date is not null and af.klaviyo_account_id is not null then af.klaviyo_account_id else null end) as signups
    ,count(distinct case when af.demo_request_date is not null and af.klaviyo_account_id is not null then af.klaviyo_account_id else null end) as demo_requests
    , round(div0(mqls, visits),4)*100 as MQL_CVR
    , round(div0(signups,visits),4)*100 as Signup_CVR
    , round(div0(demo_requests,visits),4)*100 as DemoRequests_CVR
    FROM staging.stg_ga_hits h
    LEFT JOIN public.TBL_GA_SESSIONS ga on h.full_visitor_id = ga.full_visitor_id and h.visit_id = ga.visit_id
    LEFT JOIN public.tbl_account_dimensions ad on ga.klaviyo_account_id = ad.klaviyo_account_id
    LEFT JOIN klaviyo.sandbox.tbl_acquisition_funnel af on ga.klaviyo_account_id = af.klaviyo_account_id
    where split_part(h.h__value:page.pagePath::string,'?',0)
    in (
    '/',
    '/features',
    '/features/ecommerce-marketing-automation',
    '/pricing',
    '/email-marketing',
    '/switch/from-mailchimp',
    '/ecommerce-integrations/woocommerce',
    '/switch',
    '/sms-marketing',
    '/features/templates') 
    and h.visit_start_time >= '2022-01-03' 
    and array_contains('New Visitor'::variant, ga_segment)
    GROUP BY 1,2,3

    union all 

    --------------------------Device BRKDWN---------------------------
    SELECT
    date_trunc('week',h.visit_start_time)::date as WEEK
    , 'Device' as LEVELs
    ,trim(ga.device['deviceCategory'],'"') as Brkdwn
    ,count(DISTINCT h.full_visitor_id) as visits
    ,COUNT(DISTINCT (CASE WHEN 1=1
            and h__value['eventInfo']['eventLabel'] = 'complete' 
            and h__value['eventInfo']['eventCategory'] = 'form'
            -- and regexp_like (h__value['eventInfo']['eventAction'], '(.*signup.*|.*requestDemo.*|.*demo request.*)', 'i')            
            and h__value['eventInfo']['eventAction'] in ('signup','requestDemo')
           THEN h.full_visitor_id END)) as Complete_Setup
    ,count(distinct case when af.lead_mql_date is not null and af.mql_flag = 1 and af.klaviyo_account_id is not null then af.klaviyo_account_id else null end) as mqls
    ,count(distinct case when af.klaviyo_account_created_date is not null and af.klaviyo_account_id is not null then af.klaviyo_account_id else null end) as signups
    ,count(distinct case when af.demo_request_date is not null and af.klaviyo_account_id is not null then af.klaviyo_account_id else null end) as demo_requests
    , round(div0(mqls, visits),4)*100 as MQL_CVR
    , round(div0(signups,visits),4)*100 as Signup_CVR
    , round(div0(demo_requests,visits),4)*100 as DemoRequests_CVR
    FROM staging.stg_ga_hits h
    LEFT JOIN public.TBL_GA_SESSIONS ga on h.full_visitor_id = ga.full_visitor_id and h.visit_id = ga.visit_id
    LEFT JOIN public.tbl_account_dimensions ad on ga.klaviyo_account_id = ad.klaviyo_account_id
    LEFT JOIN klaviyo.sandbox.tbl_acquisition_funnel af on ga.klaviyo_account_id = af.klaviyo_account_id
    where split_part(h.h__value:page.pagePath::string,'?',0)
    in (
    '/',
    '/features',
    '/features/ecommerce-marketing-automation',
    '/pricing',
    '/email-marketing',
    '/switch/from-mailchimp',
    '/ecommerce-integrations/woocommerce',
    '/switch',
    '/sms-marketing',
    '/features/templates') 
    and h.visit_start_time >= '2022-01-03' 
    and array_contains('New Visitor'::variant, ga_segment)
    GROUP BY 1,2,3

    union all
    --------------------------Country BRKDWN---------------------------
    SELECT
    date_trunc('week',h.visit_start_time)::date as WEEK
    ,'Country' as LEVELs
    , case when trim(ga.GEO_NETWORK['country'],'"') in ('Canada','United States')
      then trim(ga.GEO_NETWORK['country'],'"')
      else cf.geo end as Brkdwn
    ,count(DISTINCT h.full_visitor_id) as visits
    ,count(DISTINCT (CASE WHEN 1=1
            and h__value['eventInfo']['eventLabel'] = 'complete' 
            and h__value['eventInfo']['eventCategory'] = 'form'
            -- and regexp_like (h__value['eventInfo']['eventAction'], '(.*signup.*|.*requestDemo.*|.*demo request.*)', 'i')            
            and h__value['eventInfo']['eventAction'] in ('signup','requestDemo')
           THEN h.full_visitor_id END)) as Complete_Setup
    ,count(distinct case when af.lead_mql_date is not null and af.mql_flag = 1 and af.klaviyo_account_id is not null then af.klaviyo_account_id else null end) as mqls
    ,count(distinct case when af.klaviyo_account_created_date is not null and af.klaviyo_account_id is not null then af.klaviyo_account_id else null end) as signups
    ,count(distinct case when af.demo_request_date is not null and af.klaviyo_account_id is not null then af.klaviyo_account_id else null end) as demo_requests
    , round(div0(mqls, visits),4)*100 as MQL_CVR
    , round(div0(signups,visits),4)*100 as Signup_CVR
    , round(div0(demo_requests,visits),4)*100 as DemoRequests_CVR
    FROM staging.stg_ga_hits h
    LEFT JOIN public.TBL_GA_SESSIONS ga on h.full_visitor_id = ga.full_visitor_id and h.visit_id = ga.visit_id
    LEFT JOIN public.tbl_account_dimensions ad on ga.klaviyo_account_id = ad.klaviyo_account_id
    LEFT JOIN klaviyo.sandbox.tbl_acquisition_funnel af on ga.klaviyo_account_id = af.klaviyo_account_id
    LEFT JOIN country_ref cf on cf.country = trim(ga.GEO_NETWORK['country'],'"')
    where split_part(h.h__value:page.pagePath::string,'?',0)
    in (
    '/',
    '/features',
    '/features/ecommerce-marketing-automation',
    '/pricing',
    '/email-marketing',
    '/switch/from-mailchimp',
    '/ecommerce-integrations/woocommerce',
    '/switch',
    '/sms-marketing',
    '/features/templates') 
    and h.visit_start_time >= '2022-01-03' 
    and array_contains('New Visitor'::variant, ga_segment)
    and brkdwn is not null
    GROUP BY 1,2,3

    union all
    --------------------------New & Return BRKDWN---------------------------

    SELECT
    date_trunc('week',h.visit_start_time)::date as WEEK
    , 'Return_New' as LEVELs
    ,case when array_contains('New Visitor'::variant, ga_segment)
         then 'New Visitor'
         when array_contains('Returning Visitor'::variant, ga_segment)
         then 'Returning Visitor'
         else 'Others' end as Brkdwn
    ,count(DISTINCT h.full_visitor_id) as visits
    ,COUNT(DISTINCT (CASE WHEN 1=1
            and h__value['eventInfo']['eventLabel'] = 'complete' 
            and h__value['eventInfo']['eventCategory'] = 'form'
            -- and regexp_like (h__value['eventInfo']['eventAction'], '(.*signup.*|.*requestDemo.*|.*demo request.*)', 'i')            
            and h__value['eventInfo']['eventAction'] in ('signup','requestDemo')
           THEN h.full_visitor_id END)) as Complete_Setup
    ,count(distinct case when af.lead_mql_date is not null and af.mql_flag = 1 and af.klaviyo_account_id is not null then af.klaviyo_account_id else null end) as mqls
    ,count(distinct case when af.klaviyo_account_created_date is not null and af.klaviyo_account_id is not null then af.klaviyo_account_id else null end) as signups
    ,count(distinct case when af.demo_request_date is not null and af.klaviyo_account_id is not null then af.klaviyo_account_id else null end) as demo_requests
    , round(div0(mqls, visits),4)*100 as MQL_CVR
    , round(div0(signups,visits),4)*100 as Signup_CVR
    , round(div0(demo_requests,visits),4)*100 as DemoRequests_CVR
    FROM staging.stg_ga_hits h
    LEFT JOIN public.TBL_GA_SESSIONS ga on h.full_visitor_id = ga.full_visitor_id and h.visit_id = ga.visit_id
    LEFT JOIN public.tbl_account_dimensions ad on ga.klaviyo_account_id = ad.klaviyo_account_id
    LEFT JOIN klaviyo.sandbox.tbl_acquisition_funnel af on ga.klaviyo_account_id = af.klaviyo_account_id
    where split_part(h.h__value:page.pagePath::string,'?',0)
    in (
    '/',
    '/features',
    '/features/ecommerce-marketing-automation',
    '/pricing',
    '/email-marketing',
    '/switch/from-mailchimp',
    '/ecommerce-integrations/woocommerce',
    '/switch',
    '/sms-marketing',
    '/features/templates') 
    and h.visit_start_time >= '2022-01-03' 
    and Brkdwn != 'Others'
    GROUP BY 1,2,3) as Combination
order by 1 desc, 2 desc, 4 desc



