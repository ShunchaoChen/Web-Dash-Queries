--Assembled by 12 components
-- Without GMV breakdown:
--------SS_BRK_Wkly
--------SE_BRK_Wkly
--------(SS&SE)_BRK_Wkly
-- With GMV Breakdown:
-- -----Mthly (3 queries: SE, SS, Total)
------- Daily (3 queries: SE, SS, Total)
-- -----Wkly (3 queries: SE, SS, Total)

select *
from
   ( -----------------1.SS_GMV_BRK_Wkly----------------------------------
    select * from 
    (with MQL_Signups_Demo as (
    select 
        date_trunc('week', af.lead_mql_date)::date as date_time
        ,'Week' as Date_level
        , 'Self Service' as Status
        -- won is the highest level of confidence dimension to segments, if it null, use initial_gmv_bucket
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , sum(case when af.lead_mql_date is not null and af.mql_flag = 1 then 1 else 0 end) as mqls
        , sum(case when af.klaviyo_account_created_date is not null then 1 else 0 end) as signups
        , sum(case when af.demo_request_date is not null then 1 else 0 end) as demo_requests                                                                          
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where af.lead_mql_date >= '2022-01-01' 
    and af.won_opportunity_self_serve_flag = TRUE 
    and GMV is not null
    group by 1, 2, 3, 4
    order by 1 desc, 5 desc)
    ,  --unique net new website visitors (marketing/site traffic only exclude in-app traffic), regardless of GMV segments and SS/SE status
    Visits as (
    select 
        date_trunc('week', visit_date)::date as date_time,
        count(distinct user_id) as visits_static
    from klaviyo.public.bi_website_visitors 
    where visitor_type = 'Visitor'
    and visit_type = 'Net New Visitor'
    and page_classification = 'marketing site'
    and visit_date >= '2022-01-01'
    group by 1
    order by 1 desc)
    ,
    New_Logos as (
    select
        date_trunc('week', MOST_RECENT_ACQUISITION_DATE)::date as start_date
        ,CURRENT_GMV_BUCKET as GMV
        ,SUM(LOGO_NEW_COUNT) as new_Logos
    from klaviyo.public.static_subscriptions
    where coalesce(self_serve_status, 'Sales-Enabled') = 'Self-Serve' and MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
    group by 1,2
    order by 1 desc)
    ,
    -- Activated metrics combined
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
        -- won is the highest level of confidence dimension to segments, if it null, use initial_gmv_bucket
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , count(distinct af.klaviyo_account_id) as activated_static
    from klaviyo.sandbox.tbl_acquisition_funnel af
    join lead_aggregation la on af.bi_lead_id = la.bi_lead_id
    where earliest_activated_date >= '2022-01-01' 
    and GMV is not null
    and af.lead_type in ('Email', 'Both')
    group by 1,2
    order by 1 desc)
    ,
    Workable as (
    select 
        date_trunc('week', lead_workable_date)::date as workable_date
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , sum(workable_lead_flag) as workable_count_SE
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where workable_date >= '2022-01-01' 
    --Email--- 
    and lead_type in ('Email', 'Both') 
    --Inbound Only 
    and coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound' 
    --AE--
    and product_specialist_flag = 'Account Executive'
    group by 1,2
    order by 1)
    ,
    Flips as (
    --SE Inbound flips (SE default) AE Only --
    select 
        date_trunc('week', lead_flip_date)::date as flip_date
        , case when af.won_gmv_bucket is not null
            then af.won_gmv_bucket
            else af.initial_gmv_bucket end as GMV
        , sum(flipped_flag) as flip_count_SE
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where flip_date::date >= '2022-01-01' 
    --Inbound Only
    and  coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound'   
    --Email---
    and lead_type in ('Email', 'Both')
    --AE--
    and product_specialist_flag = 'Account Executive'
    group by 1,2
    order by 1
    )
    ,
    MRR_ASP as (--SS MRR_ASP
    select
        -- according to Evan Cover    
        date_trunc('week', MOST_RECENT_ACQUISITION_DATE) as start_date
        ,CURRENT_GMV_BUCKET as GMV
         --email asp numerator
        ,  sum(email_new_mrr) + sum(email_reactivation_mrr) + sum(email_additional_product_mrr) as mrr
         --email asp denominator
        , sum(email_logo_new_count)+ sum(email_logo_reactivation_count) + sum(email_logo_additional_product_count) as paying_customer_cnt
        , round(div0(MRR, paying_customer_cnt),2) as asp   
    from klaviyo.public.static_subscriptions
    where  coalesce(self_serve_status, 'Sales-Enabled') = 'Self-Serve'
    and MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
    group by 1,2
    order by 1 desc)

    select M.date_time, 
            M.Date_level,
            M.Status,
            M.GMV,
            V.visits_static, 
            A.activated_static, 
            M.mqls, 
            M.signups, 
            M.demo_requests, 
            W.workable_count_SE,
            F.flip_count_SE,
            round(div0(M.mqls,V.visits_static),4)*100 as MQL_CVR,
            round(div0(M.signups,V.visits_static),4)*100 as Signup_CVR,
            round(div0(M.demo_requests,V.visits_static),4)*100 as DemoRequests_CVR,
            L.new_Logos,
            P.paying_customer_cnt,
            P.mrr,
            P.asp
    from MQL_Signups_Demo M
    left join Visits V on M.date_time = V.date_time
    left join Activated_Final A on M.date_time = A.earliest_activated_date and M.GMV = A.GMV
    left join New_Logos L on L.start_date = M.date_time and M.GMV = L.GMV
    left join MRR_ASP P on M.date_time = P.start_date and M.GMV = P.GMV
    left join Workable W on M.date_time = W.workable_date and M.GMV = W.GMV
    left join Flips F on  M.date_time = F.flip_date and M.GMV = F.GMV)

    union all 

    -----------------2.SE_GMV_BRK_Wrkly----------------------------------
    select * from
    (with MQL_Signups_Demo as (
    select 
        date_trunc('week', af.lead_mql_date)::date as date_time
        ,'Week' as Date_level
        ,'Self-Enabled' as Status
        -- won is the highest level of confidence dimension to segments, if it null, use initial_gmv_bucket
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , sum(case when af.lead_mql_date is not null and af.mql_flag = 1 then 1 else 0 end) as mqls
        , sum(case when af.klaviyo_account_created_date is not null then 1 else 0 end) as signups
        , sum(case when af.demo_request_date is not null then 1 else 0 end) as demo_requests                                                                          
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where af.lead_mql_date >= '2022-01-01' 
    and af.won_opportunity_self_serve_flag = FALSE 
    and GMV is not null
    group by 1, 2, 3, 4
    order by 1 desc, 5 desc)
    ,  --unique net new website visitors (marketing/site traffic only exclude in-app traffic), regardless of GMV segments and SS/SE status
    Visits as (
    select 
        date_trunc('week', visit_date)::date as date_time,
        count(distinct user_id) as visits_static
    from klaviyo.public.bi_website_visitors 
    where visitor_type = 'Visitor'
    and visit_type = 'Net New Visitor'
    and page_classification = 'marketing site'
    and visit_date >= '2022-01-01'
    group by 1
    order by 1 desc)
    ,
    New_Logos as (
    select
        date_trunc('week', MOST_RECENT_ACQUISITION_DATE)::date as start_date
        ,CURRENT_GMV_BUCKET as GMV
        ,SUM(LOGO_NEW_COUNT) as new_Logos
    from klaviyo.public.static_subscriptions
    where MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
    and coalesce(self_serve_status, 'Sales-Enabled') = 'Self-Enabled' 
    group by 1,2
    order by 1 desc)
    ,
    -- Activated metrics combined
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
        -- won is the highest level of confidence dimension to segments, if it null, use initial_gmv_bucket
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , count(distinct af.klaviyo_account_id) as activated_static
    from klaviyo.sandbox.tbl_acquisition_funnel af
    join lead_aggregation la on af.bi_lead_id = la.bi_lead_id
    where earliest_activated_date >= '2022-01-01' 
    and GMV is not null
    and af.lead_type in ('Email', 'Both')
    group by 1,2
    order by 1 desc)
    ,
    Workable as (
    select 
        date_trunc('week', lead_workable_date)::date as workable_date
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , sum(workable_lead_flag) as workable_count_SE
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where workable_date >= '2022-01-01' 
    --Email--- 
    and lead_type in ('Email', 'Both') 
    --Inbound Only 
    and coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound' 
    --AE--
    and product_specialist_flag = 'Account Executive'
    group by 1,2
    order by 1)
    ,
    Flips as (
    --SE Inbound flips (SE default) AE Only --
    select 
        date_trunc('week', lead_flip_date)::date as flip_date
        , case when af.won_gmv_bucket is not null
            then af.won_gmv_bucket
            else af.initial_gmv_bucket end as GMV
        , sum(flipped_flag) as flip_count_SE
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where flip_date::date >= '2022-01-01' 
    --Inbound Only
    and  coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound'   
    --Email---
    and lead_type in ('Email', 'Both')
    --AE--
    and product_specialist_flag = 'Account Executive'
    group by 1,2
    order by 1
    )
    ,
    MRR_ASP as (--SS MRR_ASP
    select
        -- according to Evan Cover    
        date_trunc('week', MOST_RECENT_ACQUISITION_DATE) as start_date
        ,CURRENT_GMV_BUCKET as GMV
         --email asp numerator
        ,  sum(email_new_mrr) + sum(email_reactivation_mrr) + sum(email_additional_product_mrr) as mrr
         --email asp denominator
        , sum(email_logo_new_count)+ sum(email_logo_reactivation_count) + sum(email_logo_additional_product_count) as paying_customer_cnt
        , round(div0(MRR, paying_customer_cnt),2) as asp   
    from klaviyo.public.static_subscriptions
    where  1=1
    and coalesce(self_serve_status, 'Sales-Enabled') = 'Self-Enabled'
    and MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
    group by 1,2
    order by 1 desc)

    select M.date_time, 
            M.Date_level,
            M.Status,
            M.GMV,
            V.visits_static, 
            A.activated_static, 
            M.mqls, 
            M.signups, 
            M.demo_requests, 
            W.workable_count_SE,
            F.flip_count_SE,
            round(div0(M.mqls,V.visits_static),4)*100 as MQL_CVR,
            round(div0(M.signups,V.visits_static),4)*100 as Signup_CVR,
            round(div0(M.demo_requests,V.visits_static),4)*100 as DemoRequests_CVR,        
            L.new_Logos,
            P.paying_customer_cnt,
            P.mrr,
            P.asp
    from MQL_Signups_Demo M
    left join Visits V on M.date_time = V.date_time
    left join Activated_Final A on M.date_time = A.earliest_activated_date and M.GMV = A.GMV
    left join New_Logos L on L.start_date = M.date_time and M.GMV = L.GMV
    left join MRR_ASP P on M.date_time = P.start_date and M.GMV = P.GMV
    left join Workable W on M.date_time = W.workable_date and M.GMV = W.GMV
    left join Flips F on  M.date_time = F.flip_date and M.GMV = F.GMV)

    union all

    -----------------3.(SE&SS)_GMV_BRK_Wrkly----------------------------------
    select * from
    (with MQL_Signups_Demo as (
    select 
        date_trunc('week', af.lead_mql_date)::date as date_time
        ,'Week' as Date_level
        , 'All' as Status
        -- won is the highest level of confidence dimension to segments, if it null, use initial_gmv_bucket
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , sum(case when af.lead_mql_date is not null and af.mql_flag = 1 then 1 else 0 end) as mqls
        , sum(case when af.klaviyo_account_created_date is not null then 1 else 0 end) as signups
        , sum(case when af.demo_request_date is not null then 1 else 0 end) as demo_requests                                                                          
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where af.lead_mql_date >= '2022-01-01' 
    -- and af.won_opportunity_self_serve_flag = FALSE 
    and GMV is not null
    group by 1, 2, 3, 4
    order by 1 desc, 5 desc)
    ,  --unique net new website visitors (marketing/site traffic only exclude in-app traffic), regardless of GMV segments and SS/SE status
    Visits as (
    select 
        date_trunc('week', visit_date)::date as date_time,
        count(distinct user_id) as visits_static
    from klaviyo.public.bi_website_visitors 
    where visitor_type = 'Visitor'
    and visit_type = 'Net New Visitor'
    and page_classification = 'marketing site'
    and visit_date >= '2022-01-01'
    group by 1
    order by 1 desc)
    ,
    New_Logos as (
    select
        date_trunc('week', MOST_RECENT_ACQUISITION_DATE)::date as start_date
        ,CURRENT_GMV_BUCKET as GMV
        ,SUM(LOGO_NEW_COUNT) as new_Logos
    from klaviyo.public.static_subscriptions
    where 1=1
    -- and coalesce(self_serve_status, 'Sales-Enabled') = 'Self-Enabled' 
    and MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
    group by 1,2
    order by 1 desc)
    ,
    -- Activated metrics combined
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
        -- won is the highest level of confidence dimension to segments, if it null, use initial_gmv_bucket
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , count(distinct af.klaviyo_account_id) as activated_static
    from klaviyo.sandbox.tbl_acquisition_funnel af
    join lead_aggregation la on af.bi_lead_id = la.bi_lead_id
    where earliest_activated_date >= '2022-01-01' 
    and GMV is not null
    and af.lead_type in ('Email', 'Both')
    group by 1,2
    order by 1 desc)
    ,
    Workable as (
    select 
        date_trunc('week', lead_workable_date)::date as workable_date
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , sum(workable_lead_flag) as workable_count_SE
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where workable_date >= '2022-01-01' 
    --Email--- 
    and lead_type in ('Email', 'Both') 
    --Inbound Only 
    and coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound' 
    --AE--
    and product_specialist_flag = 'Account Executive'
    group by 1,2
    order by 1)
    ,
    Flips as (
    --SE Inbound flips (SE default) AE Only --
    select 
        date_trunc('week', lead_flip_date)::date as flip_date
        , case when af.won_gmv_bucket is not null
            then af.won_gmv_bucket
            else af.initial_gmv_bucket end as GMV
        , sum(flipped_flag) as flip_count_SE
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where flip_date::date >= '2022-01-01' 
    --Inbound Only
    and coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound'   
    --Email---
    and lead_type in ('Email', 'Both')
    --AE--
    and product_specialist_flag = 'Account Executive'
    group by 1,2
    order by 1
    )
    ,
    MRR_ASP as (--SS MRR_ASP
    select
        -- according to Evan Cover    
        date_trunc('week', MOST_RECENT_ACQUISITION_DATE) as start_date
        ,CURRENT_GMV_BUCKET as GMV
         --email asp numerator
        ,  sum(email_new_mrr) + sum(email_reactivation_mrr) + sum(email_additional_product_mrr) as mrr
         --email asp denominator
        , sum(email_logo_new_count)+ sum(email_logo_reactivation_count) + sum(email_logo_additional_product_count) as paying_customer_cnt
        , round(div0(MRR, paying_customer_cnt),2) as asp   
    from klaviyo.public.static_subscriptions
    where 1=1
    -- and coalesce(self_serve_status, 'Sales-Enabled') = 'Self-Enabled'
    and MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
    group by 1,2
    order by 1 desc)

    select M.date_time, 
            M.Date_level,
            M.Status,
            M.GMV,
            V.visits_static, 
            A.activated_static, 
            M.mqls, 
            M.signups, 
            M.demo_requests, 
            W.workable_count_SE,
            F.flip_count_SE,
            round(div0(M.mqls,V.visits_static),4)*100 as MQL_CVR,
            round(div0(M.signups,V.visits_static),4)*100 as Signup_CVR,
            round(div0(M.demo_requests,V.visits_static),4)*100 as DemoRequests_CVR,        
            L.new_Logos,
            P.paying_customer_cnt,
            P.mrr,
            P.asp
    from MQL_Signups_Demo M
    left join Visits V on M.date_time = V.date_time
    left join Activated_Final A on M.date_time = A.earliest_activated_date and M.GMV = A.GMV
    left join New_Logos L on L.start_date = M.date_time and M.GMV = L.GMV
    left join MRR_ASP P on M.date_time = P.start_date and M.GMV = P.GMV
    left join Workable W on M.date_time = W.workable_date and M.GMV = W.GMV
    left join Flips F on  M.date_time = F.flip_date and M.GMV = F.GMV)

    union all

    -----------------4.SS_BRK_Wkly----------------------------------
    select * from 
    (with MQL_Signups_Demo as (
    select 
        date_trunc('week', af.lead_mql_date)::date as date_time
        ,'Week' as Date_level
        , 'Self Service' as Status
        -- won is the highest level of confidence dimension to segments, if it null, use initial_gmv_bucket
        , 'Overall' as GMV
        , sum(case when af.lead_mql_date is not null and af.mql_flag = 1 then 1 else 0 end) as mqls
        , sum(case when af.klaviyo_account_created_date is not null then 1 else 0 end) as signups
        , sum(case when af.demo_request_date is not null then 1 else 0 end) as demo_requests                                                                          
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where af.lead_mql_date >= '2022-01-01' 
    and af.won_opportunity_self_serve_flag = TRUE 
    and GMV is not null
    group by 1, 2, 3, 4
    order by 1 desc, 5 desc)
    ,  --unique net new website visitors (marketing/site traffic only exclude in-app traffic), regardless of GMV segments and SS/SE status
    Visits as (
    select 
        date_trunc('week', visit_date)::date as date_time,
        count(distinct user_id) as visits_static
    from klaviyo.public.bi_website_visitors 
    where visitor_type = 'Visitor'
    and visit_type = 'Net New Visitor'
    and page_classification = 'marketing site'
    and visit_date >= '2022-01-01'
    group by 1
    order by 1 desc)
    ,
    New_Logos as (
    select
        date_trunc('week', MOST_RECENT_ACQUISITION_DATE)::date as start_date
        ,'Overall' as GMV
        ,SUM(LOGO_NEW_COUNT) as new_Logos
    from klaviyo.public.static_subscriptions
    where coalesce(self_serve_status, 'Sales-Enabled') = 'Self-Serve' and MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
    group by 1,2
    order by 1 desc)
    ,
    -- Activated metrics combined
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
        -- won is the highest level of confidence dimension to segments, if it null, use initial_gmv_bucket
        , 'Overall' as GMV
        , count(distinct af.klaviyo_account_id) as activated_static
    from klaviyo.sandbox.tbl_acquisition_funnel af
    join lead_aggregation la on af.bi_lead_id = la.bi_lead_id
    where earliest_activated_date >= '2022-01-01' 
    and GMV is not null
    and af.lead_type in ('Email', 'Both')
    group by 1,2
    order by 1 desc)
    ,
    Workable as (
    select 
        date_trunc('week', lead_workable_date)::date as workable_date
        , 'Overall' as GMV
        , sum(workable_lead_flag) as workable_count_SE
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where workable_date >= '2022-01-01' 
    --Email--- 
    and lead_type in ('Email', 'Both') 
    --Inbound Only 
    and coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound' 
    --AE--
    and product_specialist_flag = 'Account Executive'
    group by 1,2
    order by 1)
    ,
    Flips as (
    --SE Inbound flips (SE default) AE Only --
    select 
        date_trunc('week', lead_flip_date)::date as flip_date
        , 'Overall' as GMV
        , sum(flipped_flag) as flip_count_SE
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where flip_date::date >= '2022-01-01' 
    --Inbound Only
    and  coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound'   
    --Email---
    and lead_type in ('Email', 'Both')
    --AE--
    and product_specialist_flag = 'Account Executive'
    group by 1,2
    order by 1
    )
    ,
    MRR_ASP as (--SS MRR_ASP
    select
        -- according to Evan Cover    
        date_trunc('week', MOST_RECENT_ACQUISITION_DATE) as start_date
        ,'Overall' as GMV
         --email asp numerator
        ,  sum(email_new_mrr) + sum(email_reactivation_mrr) + sum(email_additional_product_mrr) as mrr
         --email asp denominator
        , sum(email_logo_new_count)+ sum(email_logo_reactivation_count) + sum(email_logo_additional_product_count) as paying_customer_cnt
        , round(div0(MRR, paying_customer_cnt),2) as asp   
    from klaviyo.public.static_subscriptions
    where  coalesce(self_serve_status, 'Sales-Enabled') = 'Self-Serve'
    and MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
    group by 1,2
    order by 1 desc)

    select M.date_time, 
            M.Date_level,
            M.Status,
            M.GMV,
            V.visits_static, 
            A.activated_static, 
            M.mqls, 
            M.signups, 
            M.demo_requests, 
            W.workable_count_SE,
            F.flip_count_SE,
            round(div0(M.mqls,V.visits_static),4)*100 as MQL_CVR,
            round(div0(M.signups,V.visits_static),4)*100 as Signup_CVR,
            round(div0(M.demo_requests,V.visits_static),4)*100 as DemoRequests_CVR,
            L.new_Logos,
            P.paying_customer_cnt,
            P.mrr,
            P.asp
    from MQL_Signups_Demo M
    left join Visits V on M.date_time = V.date_time
    left join Activated_Final A on M.date_time = A.earliest_activated_date and M.GMV = A.GMV
    left join New_Logos L on L.start_date = M.date_time and M.GMV = L.GMV
    left join MRR_ASP P on M.date_time = P.start_date and M.GMV = P.GMV
    left join Workable W on M.date_time = W.workable_date and M.GMV = W.GMV
    left join Flips F on  M.date_time = F.flip_date and M.GMV = F.GMV)

    union all

    -----------------5.SE_BRK_Wkly----------------------------------
    select * from 
    (with MQL_Signups_Demo as (
    select 
        date_trunc('week', af.lead_mql_date)::date as date_time
        ,'Week' as Date_level
        , 'Self-Enabled' as Status
        -- won is the highest level of confidence dimension to segments, if it null, use initial_gmv_bucket
        , 'Overall' as GMV
        , sum(case when af.lead_mql_date is not null and af.mql_flag = 1 then 1 else 0 end) as mqls
        , sum(case when af.klaviyo_account_created_date is not null then 1 else 0 end) as signups
        , sum(case when af.demo_request_date is not null then 1 else 0 end) as demo_requests                                                                          
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where af.lead_mql_date >= '2022-01-01' 
    and af.won_opportunity_self_serve_flag = FALSE 
    and GMV is not null
    group by 1, 2, 3, 4
    order by 1 desc, 5 desc)
    ,  --unique net new website visitors (marketing/site traffic only exclude in-app traffic), regardless of GMV segments and SS/SE status
    Visits as (
    select 
        date_trunc('week', visit_date)::date as date_time,
        count(distinct user_id) as visits_static
    from klaviyo.public.bi_website_visitors 
    where visitor_type = 'Visitor'
    and visit_type = 'Net New Visitor'
    and page_classification = 'marketing site'
    and visit_date >= '2022-01-01'
    group by 1
    order by 1 desc)
    ,
    New_Logos as (
    select
        date_trunc('week', MOST_RECENT_ACQUISITION_DATE)::date as start_date
        ,'Overall' as GMV
        ,SUM(LOGO_NEW_COUNT) as new_Logos
    from klaviyo.public.static_subscriptions
    where coalesce(self_serve_status, 'Sales-Enabled') = 'Self-Enabled' and MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
    group by 1,2
    order by 1 desc)
    ,
    -- Activated metrics combined
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
        -- won is the highest level of confidence dimension to segments, if it null, use initial_gmv_bucket
        , 'Overall' as GMV
        , count(distinct af.klaviyo_account_id) as activated_static
    from klaviyo.sandbox.tbl_acquisition_funnel af
    join lead_aggregation la on af.bi_lead_id = la.bi_lead_id
    where earliest_activated_date >= '2022-01-01' 
    and GMV is not null
    and af.lead_type in ('Email', 'Both')
    group by 1,2
    order by 1 desc)
    ,
    Workable as (
    select 
        date_trunc('week', lead_workable_date)::date as workable_date
        , 'Overall' as GMV
        , sum(workable_lead_flag) as workable_count_SE
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where workable_date >= '2022-01-01' 
    --Email--- 
    and lead_type in ('Email', 'Both') 
    --Inbound Only 
    and coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound' 
    --AE--
    and product_specialist_flag = 'Account Executive'
    group by 1,2
    order by 1)
    ,
    Flips as (
    --SE Inbound flips (SE default) AE Only --
    select 
        date_trunc('week', lead_flip_date)::date as flip_date
        , 'Overall' as GMV
        , sum(flipped_flag) as flip_count_SE
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where flip_date::date >= '2022-01-01' 
    --Inbound Only
    and  coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound'   
    --Email---
    and lead_type in ('Email', 'Both')
    --AE--
    and product_specialist_flag = 'Account Executive'
    group by 1,2
    order by 1
    )
    ,
    MRR_ASP as (--SS MRR_ASP
    select
        -- according to Evan Cover    
        date_trunc('week', MOST_RECENT_ACQUISITION_DATE) as start_date
        ,'Overall' as GMV
         --email asp numerator
        ,  sum(email_new_mrr) + sum(email_reactivation_mrr) + sum(email_additional_product_mrr) as mrr
         --email asp denominator
        , sum(email_logo_new_count)+ sum(email_logo_reactivation_count) + sum(email_logo_additional_product_count) as paying_customer_cnt
        , round(div0(MRR, paying_customer_cnt),2) as asp   
    from klaviyo.public.static_subscriptions
    where  coalesce(self_serve_status, 'Sales-Enabled') = 'Self-Enabled'
    and MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
    group by 1,2
    order by 1 desc)

    select M.date_time, 
            M.Date_level,
            M.Status,
            M.GMV,
            V.visits_static, 
            A.activated_static, 
            M.mqls, 
            M.signups, 
            M.demo_requests, 
            W.workable_count_SE,
            F.flip_count_SE,
            round(div0(M.mqls,V.visits_static),4)*100 as MQL_CVR,
            round(div0(M.signups,V.visits_static),4)*100 as Signup_CVR,
            round(div0(M.demo_requests,V.visits_static),4)*100 as DemoRequests_CVR,
            L.new_Logos,
            P.paying_customer_cnt,
            P.mrr,
            P.asp
    from MQL_Signups_Demo M
    left join Visits V on M.date_time = V.date_time
    left join Activated_Final A on M.date_time = A.earliest_activated_date and M.GMV = A.GMV
    left join New_Logos L on L.start_date = M.date_time and M.GMV = L.GMV
    left join MRR_ASP P on M.date_time = P.start_date and M.GMV = P.GMV
    left join Workable W on M.date_time = W.workable_date and M.GMV = W.GMV
    left join Flips F on  M.date_time = F.flip_date and M.GMV = F.GMV)

    union all 
    -----------------6.(SE&SS)_BRK_Wkly----------------------------------
    select * from 
    (with MQL_Signups_Demo as (
    select 
        date_trunc('week', af.lead_mql_date)::date as date_time
        ,'Week' as Date_level
        , 'All' as Status
        -- won is the highest level of confidence dimension to segments, if it null, use initial_gmv_bucket
        , 'Overall' as GMV
        , sum(case when af.lead_mql_date is not null and af.mql_flag = 1 then 1 else 0 end) as mqls
        , sum(case when af.klaviyo_account_created_date is not null then 1 else 0 end) as signups
        , sum(case when af.demo_request_date is not null then 1 else 0 end) as demo_requests                                                                          
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where af.lead_mql_date >= '2022-01-01' 
    -- and af.won_opportunity_self_serve_flag = FALSE 
    and GMV is not null
    group by 1, 2, 3, 4
    order by 1 desc, 5 desc)
    ,  --unique net new website visitors (marketing/site traffic only exclude in-app traffic), regardless of GMV segments and SS/SE status
    Visits as (
    select 
        date_trunc('week', visit_date)::date as date_time,
        count(distinct user_id) as visits_static
    from klaviyo.public.bi_website_visitors 
    where visitor_type = 'Visitor'
    and visit_type = 'Net New Visitor'
    and page_classification = 'marketing site'
    and visit_date >= '2022-01-01'
    group by 1
    order by 1 desc)
    ,
    New_Logos as (
    select
        date_trunc('week', MOST_RECENT_ACQUISITION_DATE)::date as start_date
        ,'Overall' as GMV
        ,SUM(LOGO_NEW_COUNT) as new_Logos
    from klaviyo.public.static_subscriptions
    where 1=1
    -- and coalesce(self_serve_status, 'Sales-Enabled') = 'Self-Enabled' 
    and MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
    group by 1,2
    order by 1 desc)
    ,
    -- Activated metrics combined
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
        -- won is the highest level of confidence dimension to segments, if it null, use initial_gmv_bucket
        , 'Overall' as GMV
        , count(distinct af.klaviyo_account_id) as activated_static
    from klaviyo.sandbox.tbl_acquisition_funnel af
    join lead_aggregation la on af.bi_lead_id = la.bi_lead_id
    where earliest_activated_date >= '2022-01-01' 
    and GMV is not null
    and af.lead_type in ('Email', 'Both')
    group by 1,2
    order by 1 desc)
    ,
    Workable as (
    select 
        date_trunc('week', lead_workable_date)::date as workable_date
        , 'Overall' as GMV
        , sum(workable_lead_flag) as workable_count_SE
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where workable_date >= '2022-01-01' 
    --Email--- 
    and lead_type in ('Email', 'Both') 
    --Inbound Only 
    and coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound' 
    --AE--
    and product_specialist_flag = 'Account Executive'
    group by 1,2
    order by 1)
    ,
    Flips as (
    --SE Inbound flips (SE default) AE Only --
    select 
        date_trunc('week', lead_flip_date)::date as flip_date
        , 'Overall' as GMV
        , sum(flipped_flag) as flip_count_SE
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where flip_date::date >= '2022-01-01' 
    --Inbound Only
    and  coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound'   
    --Email---
    and lead_type in ('Email', 'Both')
    --AE--
    and product_specialist_flag = 'Account Executive'
    group by 1,2
    order by 1
    )
    ,
    MRR_ASP as (--SS MRR_ASP
    select
        -- according to Evan Cover    
        date_trunc('week', MOST_RECENT_ACQUISITION_DATE) as start_date
        ,'Overall' as GMV
         --email asp numerator
        ,  sum(email_new_mrr) + sum(email_reactivation_mrr) + sum(email_additional_product_mrr) as mrr
         --email asp denominator
        , sum(email_logo_new_count)+ sum(email_logo_reactivation_count) + sum(email_logo_additional_product_count) as paying_customer_cnt
        , round(div0(MRR, paying_customer_cnt),2) as asp   
    from klaviyo.public.static_subscriptions
    where 1=1
    -- and coalesce(self_serve_status, 'Sales-Enabled') = 'Self-Enabled'
    and MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
    group by 1,2
    order by 1 desc)

    select M.date_time, 
            M.Date_level,
            M.Status,
            M.GMV,
            V.visits_static, 
            A.activated_static, 
            M.mqls, 
            M.signups, 
            M.demo_requests, 
            W.workable_count_SE,
            F.flip_count_SE,
            round(div0(M.mqls,V.visits_static),4)*100 as MQL_CVR,
            round(div0(M.signups,V.visits_static),4)*100 as Signup_CVR,
            round(div0(M.demo_requests,V.visits_static),4)*100 as DemoRequests_CVR,
            L.new_Logos,
            P.paying_customer_cnt,
            P.mrr,
            P.asp
    from MQL_Signups_Demo M
    left join Visits V on M.date_time = V.date_time
    left join Activated_Final A on M.date_time = A.earliest_activated_date and M.GMV = A.GMV
    left join New_Logos L on L.start_date = M.date_time and M.GMV = L.GMV
    left join MRR_ASP P on M.date_time = P.start_date and M.GMV = P.GMV
    left join Workable W on M.date_time = W.workable_date and M.GMV = W.GMV
    left join Flips F on  M.date_time = F.flip_date and M.GMV = F.GMV)

    union all
    ------------------Mthly -----------------------------------------
    -----------------7.SS_GMV_BRK_Mthly----------------------------------
    select * from 
    (with MQL_Signups_Demo as (
    select 
        date_trunc('month', af.lead_mql_date)::date as date_time
        ,'Month' as Date_level
        , 'Self Service' as Status
        -- won is the highest level of confidence dimension to segments, if it null, use initial_gmv_bucket
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , sum(case when af.lead_mql_date is not null and af.mql_flag = 1 then 1 else 0 end) as mqls
        , sum(case when af.klaviyo_account_created_date is not null then 1 else 0 end) as signups
        , sum(case when af.demo_request_date is not null then 1 else 0 end) as demo_requests                                                                          
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where af.lead_mql_date >= '2022-01-01' 
    and af.won_opportunity_self_serve_flag = TRUE 
    and GMV is not null
    group by 1, 2, 3, 4
    order by 1 desc, 5 desc)
    ,  --unique net new website visitors (marketing/site traffic only exclude in-app traffic), regardless of GMV segments and SS/SE status
    Visits as (
    select 
        date_trunc('month', visit_date)::date as date_time,
        count(distinct user_id) as visits_static
    from klaviyo.public.bi_website_visitors 
    where visitor_type = 'Visitor'
    and visit_type = 'Net New Visitor'
    and page_classification = 'marketing site'
    and visit_date >= '2022-01-01'
    group by 1
    order by 1 desc)
    ,
    New_Logos as (
    select
        date_trunc('month', MOST_RECENT_ACQUISITION_DATE)::date as start_date
        ,CURRENT_GMV_BUCKET as GMV
        ,SUM(LOGO_NEW_COUNT) as new_Logos
    from klaviyo.public.static_subscriptions
    where coalesce(self_serve_status, 'Sales-Enabled') = 'Self-Serve' and MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
    group by 1,2
    order by 1 desc)
    ,
    -- Activated metrics combined
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
        date_trunc('month', la.min_activated_date)::date as earliest_activated_date
        -- won is the highest level of confidence dimension to segments, if it null, use initial_gmv_bucket
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , count(distinct af.klaviyo_account_id) as activated_static
    from klaviyo.sandbox.tbl_acquisition_funnel af
    join lead_aggregation la on af.bi_lead_id = la.bi_lead_id
    where earliest_activated_date >= '2022-01-01' 
    and GMV is not null
    and af.lead_type in ('Email', 'Both')
    group by 1,2
    order by 1 desc)
    ,
    Workable as (
    select 
        date_trunc('month', lead_workable_date)::date as workable_date
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , sum(workable_lead_flag) as workable_count_SE
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where workable_date >= '2022-01-01' 
    --Email--- 
    and lead_type in ('Email', 'Both') 
    --Inbound Only 
    and coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound' 
    --AE--
    and product_specialist_flag = 'Account Executive'
    group by 1,2
    order by 1)
    ,
    Flips as (
    --SE Inbound flips (SE default) AE Only --
    select 
        date_trunc('month', lead_flip_date)::date as flip_date
        , case when af.won_gmv_bucket is not null
            then af.won_gmv_bucket
            else af.initial_gmv_bucket end as GMV
        , sum(flipped_flag) as flip_count_SE
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where flip_date::date >= '2022-01-01' 
    --Inbound Only
    and  coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound'   
    --Email---
    and lead_type in ('Email', 'Both')
    --AE--
    and product_specialist_flag = 'Account Executive'
    group by 1,2
    order by 1
    )
    ,
    MRR_ASP as (--SS MRR_ASP
    select
        -- according to Evan Cover    
        date_trunc('month', MOST_RECENT_ACQUISITION_DATE) as start_date
        ,CURRENT_GMV_BUCKET as GMV
         --email asp numerator
        ,  sum(email_new_mrr) + sum(email_reactivation_mrr) + sum(email_additional_product_mrr) as mrr
         --email asp denominator
        , sum(email_logo_new_count)+ sum(email_logo_reactivation_count) + sum(email_logo_additional_product_count) as paying_customer_cnt
        , round(div0(MRR, paying_customer_cnt),2) as asp   
    from klaviyo.public.static_subscriptions
    where  coalesce(self_serve_status, 'Sales-Enabled') = 'Self-Serve'
    and MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
    group by 1,2
    order by 1 desc)

    select M.date_time, 
            M.Date_level,
            M.Status,
            M.GMV,
            V.visits_static, 
            A.activated_static, 
            M.mqls, 
            M.signups, 
            M.demo_requests, 
            W.workable_count_SE,
            F.flip_count_SE,
            round(div0(M.mqls,V.visits_static),4)*100 as MQL_CVR,
            round(div0(M.signups,V.visits_static),4)*100 as Signup_CVR,
            round(div0(M.demo_requests,V.visits_static),4)*100 as DemoRequests_CVR,
            L.new_Logos,
            P.paying_customer_cnt,
            P.mrr,
            P.asp
    from MQL_Signups_Demo M
    left join Visits V on M.date_time = V.date_time
    left join Activated_Final A on M.date_time = A.earliest_activated_date and M.GMV = A.GMV
    left join New_Logos L on L.start_date = M.date_time and M.GMV = L.GMV
    left join MRR_ASP P on M.date_time = P.start_date and M.GMV = P.GMV
    left join Workable W on M.date_time = W.workable_date and M.GMV = W.GMV
    left join Flips F on  M.date_time = F.flip_date and M.GMV = F.GMV)

    union all 

    -----------------8.SE_GMV_BRK_Mthly----------------------------------
    select * from
    (with MQL_Signups_Demo as (
    select 
        date_trunc('month', af.lead_mql_date)::date as date_time
        ,'Month' as Date_level
        ,'Self-Enabled' as Status
        -- won is the highest level of confidence dimension to segments, if it null, use initial_gmv_bucket
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , sum(case when af.lead_mql_date is not null and af.mql_flag = 1 then 1 else 0 end) as mqls
        , sum(case when af.klaviyo_account_created_date is not null then 1 else 0 end) as signups
        , sum(case when af.demo_request_date is not null then 1 else 0 end) as demo_requests                                                                          
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where af.lead_mql_date >= '2022-01-01' 
    and af.won_opportunity_self_serve_flag = FALSE 
    and GMV is not null
    group by 1, 2, 3, 4
    order by 1 desc, 5 desc)
    ,  --unique net new website visitors (marketing/site traffic only exclude in-app traffic), regardless of GMV segments and SS/SE status
    Visits as (
    select 
        date_trunc('month', visit_date)::date as date_time,
        count(distinct user_id) as visits_static
    from klaviyo.public.bi_website_visitors 
    where visitor_type = 'Visitor'
    and visit_type = 'Net New Visitor'
    and page_classification = 'marketing site'
    and visit_date >= '2022-01-01'
    group by 1
    order by 1 desc)
    ,
    New_Logos as (
    select
        date_trunc('month', MOST_RECENT_ACQUISITION_DATE)::date as start_date
        ,CURRENT_GMV_BUCKET as GMV
        ,SUM(LOGO_NEW_COUNT) as new_Logos
    from klaviyo.public.static_subscriptions
    where MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
    and coalesce(self_serve_status, 'Sales-Enabled') = 'Self-Enabled' 
    group by 1,2
    order by 1 desc)
    ,
    -- Activated metrics combined
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
        date_trunc('month', la.min_activated_date)::date as earliest_activated_date
        -- won is the highest level of confidence dimension to segments, if it null, use initial_gmv_bucket
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , count(distinct af.klaviyo_account_id) as activated_static
    from klaviyo.sandbox.tbl_acquisition_funnel af
    join lead_aggregation la on af.bi_lead_id = la.bi_lead_id
    where earliest_activated_date >= '2022-01-01' 
    and GMV is not null
    and af.lead_type in ('Email', 'Both')
    group by 1,2
    order by 1 desc)
    ,
    Workable as (
    select 
        date_trunc('month', lead_workable_date)::date as workable_date
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , sum(workable_lead_flag) as workable_count_SE
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where workable_date >= '2022-01-01' 
    --Email--- 
    and lead_type in ('Email', 'Both') 
    --Inbound Only 
    and coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound' 
    --AE--
    and product_specialist_flag = 'Account Executive'
    group by 1,2
    order by 1)
    ,
    Flips as (
    --SE Inbound flips (SE default) AE Only --
    select 
        date_trunc('month', lead_flip_date)::date as flip_date
        , case when af.won_gmv_bucket is not null
            then af.won_gmv_bucket
            else af.initial_gmv_bucket end as GMV
        , sum(flipped_flag) as flip_count_SE
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where flip_date::date >= '2022-01-01' 
    --Inbound Only
    and  coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound'   
    --Email---
    and lead_type in ('Email', 'Both')
    --AE--
    and product_specialist_flag = 'Account Executive'
    group by 1,2
    order by 1
    )
    ,
    MRR_ASP as (--SS MRR_ASP
    select
        -- according to Evan Cover    
        date_trunc('month', MOST_RECENT_ACQUISITION_DATE) as start_date
        ,CURRENT_GMV_BUCKET as GMV
         --email asp numerator
        ,  sum(email_new_mrr) + sum(email_reactivation_mrr) + sum(email_additional_product_mrr) as mrr
         --email asp denominator
        , sum(email_logo_new_count)+ sum(email_logo_reactivation_count) + sum(email_logo_additional_product_count) as paying_customer_cnt
        , round(div0(MRR, paying_customer_cnt),2) as asp   
    from klaviyo.public.static_subscriptions
    where  1=1
    and coalesce(self_serve_status, 'Sales-Enabled') = 'Self-Enabled'
    and MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
    group by 1,2
    order by 1 desc)

    select M.date_time, 
            M.Date_level,
            M.Status,
            M.GMV,
            V.visits_static, 
            A.activated_static, 
            M.mqls, 
            M.signups, 
            M.demo_requests, 
            W.workable_count_SE,
            F.flip_count_SE,
            round(div0(M.mqls,V.visits_static),4)*100 as MQL_CVR,
            round(div0(M.signups,V.visits_static),4)*100 as Signup_CVR,
            round(div0(M.demo_requests,V.visits_static),4)*100 as DemoRequests_CVR,        
            L.new_Logos,
            P.paying_customer_cnt,
            P.mrr,
            P.asp
    from MQL_Signups_Demo M
    left join Visits V on M.date_time = V.date_time
    left join Activated_Final A on M.date_time = A.earliest_activated_date and M.GMV = A.GMV
    left join New_Logos L on L.start_date = M.date_time and M.GMV = L.GMV
    left join MRR_ASP P on M.date_time = P.start_date and M.GMV = P.GMV
    left join Workable W on M.date_time = W.workable_date and M.GMV = W.GMV
    left join Flips F on  M.date_time = F.flip_date and M.GMV = F.GMV)

    union all

    -----------------9.(SE&SS)_GMV_BRK_Mthly----------------------------------
    select * from
    (with MQL_Signups_Demo as (
    select 
        date_trunc('month', af.lead_mql_date)::date as date_time
        ,'Month' as Date_level
        , 'All' as Status
        -- won is the highest level of confidence dimension to segments, if it null, use initial_gmv_bucket
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , sum(case when af.lead_mql_date is not null and af.mql_flag = 1 then 1 else 0 end) as mqls
        , sum(case when af.klaviyo_account_created_date is not null then 1 else 0 end) as signups
        , sum(case when af.demo_request_date is not null then 1 else 0 end) as demo_requests                                                                          
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where af.lead_mql_date >= '2022-01-01' 
    -- and af.won_opportunity_self_serve_flag = FALSE 
    and GMV is not null
    group by 1, 2, 3, 4
    order by 1 desc, 5 desc)
    ,  --unique net new website visitors (marketing/site traffic only exclude in-app traffic), regardless of GMV segments and SS/SE status
    Visits as (
    select 
        date_trunc('month', visit_date)::date as date_time,
        count(distinct user_id) as visits_static
    from klaviyo.public.bi_website_visitors 
    where visitor_type = 'Visitor'
    and visit_type = 'Net New Visitor'
    and page_classification = 'marketing site'
    and visit_date >= '2022-01-01'
    group by 1
    order by 1 desc)
    ,
    New_Logos as (
    select
        date_trunc('month', MOST_RECENT_ACQUISITION_DATE)::date as start_date
        ,CURRENT_GMV_BUCKET as GMV
        ,SUM(LOGO_NEW_COUNT) as new_Logos
    from klaviyo.public.static_subscriptions
    where 1=1
    -- and coalesce(self_serve_status, 'Sales-Enabled') = 'Self-Enabled' 
    and MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
    group by 1,2
    order by 1 desc)
    ,
    -- Activated metrics combined
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
        date_trunc('month', la.min_activated_date)::date as earliest_activated_date
        -- won is the highest level of confidence dimension to segments, if it null, use initial_gmv_bucket
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , count(distinct af.klaviyo_account_id) as activated_static
    from klaviyo.sandbox.tbl_acquisition_funnel af
    join lead_aggregation la on af.bi_lead_id = la.bi_lead_id
    where earliest_activated_date >= '2022-01-01' 
    and GMV is not null
    and af.lead_type in ('Email', 'Both')
    group by 1,2
    order by 1 desc)
    ,
    Workable as (
    select 
        date_trunc('month', lead_workable_date)::date as workable_date
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , sum(workable_lead_flag) as workable_count_SE
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where workable_date >= '2022-01-01' 
    --Email--- 
    and lead_type in ('Email', 'Both') 
    --Inbound Only 
    and coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound' 
    --AE--
    and product_specialist_flag = 'Account Executive'
    group by 1,2
    order by 1)
    ,
    Flips as (
    --SE Inbound flips (SE default) AE Only --
    select 
        date_trunc('month', lead_flip_date)::date as flip_date
        , case when af.won_gmv_bucket is not null
            then af.won_gmv_bucket
            else af.initial_gmv_bucket end as GMV
        , sum(flipped_flag) as flip_count_SE
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where flip_date::date >= '2022-01-01' 
    --Inbound Only
    and coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound'   
    --Email---
    and lead_type in ('Email', 'Both')
    --AE--
    and product_specialist_flag = 'Account Executive'
    group by 1,2
    order by 1
    )
    ,
    MRR_ASP as (--SS MRR_ASP
    select
        -- according to Evan Cover    
        date_trunc('month', MOST_RECENT_ACQUISITION_DATE) as start_date
        ,CURRENT_GMV_BUCKET as GMV
         --email asp numerator
        ,  sum(email_new_mrr) + sum(email_reactivation_mrr) + sum(email_additional_product_mrr) as mrr
         --email asp denominator
        , sum(email_logo_new_count)+ sum(email_logo_reactivation_count) + sum(email_logo_additional_product_count) as paying_customer_cnt
        , round(div0(MRR, paying_customer_cnt),2) as asp   
    from klaviyo.public.static_subscriptions
    where 1=1
    -- and coalesce(self_serve_status, 'Sales-Enabled') = 'Self-Enabled'
    and MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
    group by 1,2
    order by 1 desc)

    select M.date_time, 
            M.Date_level,
            M.Status,
            M.GMV,
            V.visits_static, 
            A.activated_static, 
            M.mqls, 
            M.signups, 
            M.demo_requests, 
            W.workable_count_SE,
            F.flip_count_SE,
            round(div0(M.mqls,V.visits_static),4)*100 as MQL_CVR,
            round(div0(M.signups,V.visits_static),4)*100 as Signup_CVR,
            round(div0(M.demo_requests,V.visits_static),4)*100 as DemoRequests_CVR,        
            L.new_Logos,
            P.paying_customer_cnt,
            P.mrr,
            P.asp
    from MQL_Signups_Demo M
    left join Visits V on M.date_time = V.date_time
    left join Activated_Final A on M.date_time = A.earliest_activated_date and M.GMV = A.GMV
    left join New_Logos L on L.start_date = M.date_time and M.GMV = L.GMV
    left join MRR_ASP P on M.date_time = P.start_date and M.GMV = P.GMV
    left join Workable W on M.date_time = W.workable_date and M.GMV = W.GMV
    left join Flips F on  M.date_time = F.flip_date and M.GMV = F.GMV)

    union all
    ------------------Daily -----------------------------------------
    -----------------10.SS_GMV_BRK_Daily----------------------------------
    select * from 
    (with MQL_Signups_Demo as (
    select 
        date_trunc('day', af.lead_mql_date)::date as date_time
        ,'Daily' as Date_level
        , 'Self Service' as Status
        -- won is the highest level of confidence dimension to segments, if it null, use initial_gmv_bucket
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , sum(case when af.lead_mql_date is not null and af.mql_flag = 1 then 1 else 0 end) as mqls
        , sum(case when af.klaviyo_account_created_date is not null then 1 else 0 end) as signups
        , sum(case when af.demo_request_date is not null then 1 else 0 end) as demo_requests                                                                          
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where af.lead_mql_date >= '2022-01-01' 
    and af.won_opportunity_self_serve_flag = TRUE 
    and GMV is not null
    group by 1, 2, 3, 4
    order by 1 desc, 5 desc)
    ,  --unique net new website visitors (marketing/site traffic only exclude in-app traffic), regardless of GMV segments and SS/SE status
    Visits as (
    select 
        date_trunc('day', visit_date)::date as date_time,
        count(distinct user_id) as visits_static
    from klaviyo.public.bi_website_visitors 
    where visitor_type = 'Visitor'
    and visit_type = 'Net New Visitor'
    and page_classification = 'marketing site'
    and visit_date >= '2022-01-01'
    group by 1
    order by 1 desc)
    ,
    New_Logos as (
    select
        date_trunc('day', MOST_RECENT_ACQUISITION_DATE)::date as start_date
        ,CURRENT_GMV_BUCKET as GMV
        ,SUM(LOGO_NEW_COUNT) as new_Logos
    from klaviyo.public.static_subscriptions
    where coalesce(self_serve_status, 'Sales-Enabled') = 'Self-Serve' and MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
    group by 1,2
    order by 1 desc)
    ,
    -- Activated metrics combined
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
        date_trunc('day', la.min_activated_date)::date as earliest_activated_date
        -- won is the highest level of confidence dimension to segments, if it null, use initial_gmv_bucket
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , count(distinct af.klaviyo_account_id) as activated_static
    from klaviyo.sandbox.tbl_acquisition_funnel af
    join lead_aggregation la on af.bi_lead_id = la.bi_lead_id
    where earliest_activated_date >= '2022-01-01' 
    and GMV is not null
    and af.lead_type in ('Email', 'Both')
    group by 1,2
    order by 1 desc)
    ,
    Workable as (
    select 
        date_trunc('day', lead_workable_date)::date as workable_date
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , sum(workable_lead_flag) as workable_count_SE
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where workable_date >= '2022-01-01' 
    --Email--- 
    and lead_type in ('Email', 'Both') 
    --Inbound Only 
    and coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound' 
    --AE--
    and product_specialist_flag = 'Account Executive'
    group by 1,2
    order by 1)
    ,
    Flips as (
    --SE Inbound flips (SE default) AE Only --
    select 
        date_trunc('day', lead_flip_date)::date as flip_date
        , case when af.won_gmv_bucket is not null
            then af.won_gmv_bucket
            else af.initial_gmv_bucket end as GMV
        , sum(flipped_flag) as flip_count_SE
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where flip_date::date >= '2022-01-01' 
    --Inbound Only
    and  coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound'   
    --Email---
    and lead_type in ('Email', 'Both')
    --AE--
    and product_specialist_flag = 'Account Executive'
    group by 1,2
    order by 1
    )
    ,
    MRR_ASP as (--SS MRR_ASP
    select
        -- according to Evan Cover    
        date_trunc('day', MOST_RECENT_ACQUISITION_DATE) as start_date
        ,CURRENT_GMV_BUCKET as GMV
         --email asp numerator
        ,  sum(email_new_mrr) + sum(email_reactivation_mrr) + sum(email_additional_product_mrr) as mrr
         --email asp denominator
        , sum(email_logo_new_count)+ sum(email_logo_reactivation_count) + sum(email_logo_additional_product_count) as paying_customer_cnt
        , round(div0(MRR, paying_customer_cnt),2) as asp   
    from klaviyo.public.static_subscriptions
    where  coalesce(self_serve_status, 'Sales-Enabled') = 'Self-Serve'
    and MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
    group by 1,2
    order by 1 desc)

    select M.date_time, 
            M.Date_level,
            M.Status,
            M.GMV,
            V.visits_static, 
            A.activated_static, 
            M.mqls, 
            M.signups, 
            M.demo_requests, 
            W.workable_count_SE,
            F.flip_count_SE,
            round(div0(M.mqls,V.visits_static),4)*100 as MQL_CVR,
            round(div0(M.signups,V.visits_static),4)*100 as Signup_CVR,
            round(div0(M.demo_requests,V.visits_static),4)*100 as DemoRequests_CVR,
            L.new_Logos,
            P.paying_customer_cnt,
            P.mrr,
            P.asp
    from MQL_Signups_Demo M
    left join Visits V on M.date_time = V.date_time
    left join Activated_Final A on M.date_time = A.earliest_activated_date and M.GMV = A.GMV
    left join New_Logos L on L.start_date = M.date_time and M.GMV = L.GMV
    left join MRR_ASP P on M.date_time = P.start_date and M.GMV = P.GMV
    left join Workable W on M.date_time = W.workable_date and M.GMV = W.GMV
    left join Flips F on  M.date_time = F.flip_date and M.GMV = F.GMV)

    union all 

    -----------------11.SE_GMV_BRK_Daily----------------------------------
    select * from
    (with MQL_Signups_Demo as (
    select 
        date_trunc('day', af.lead_mql_date)::date as date_time
        ,'Daily' as Date_level
        ,'Self-Enabled' as Status
        -- won is the highest level of confidence dimension to segments, if it null, use initial_gmv_bucket
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , sum(case when af.lead_mql_date is not null and af.mql_flag = 1 then 1 else 0 end) as mqls
        , sum(case when af.klaviyo_account_created_date is not null then 1 else 0 end) as signups
        , sum(case when af.demo_request_date is not null then 1 else 0 end) as demo_requests                                                                          
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where af.lead_mql_date >= '2022-01-01' 
    and af.won_opportunity_self_serve_flag = FALSE 
    and GMV is not null
    group by 1, 2, 3, 4
    order by 1 desc, 5 desc)
    ,  --unique net new website visitors (marketing/site traffic only exclude in-app traffic), regardless of GMV segments and SS/SE status
    Visits as (
    select 
        date_trunc('day', visit_date)::date as date_time,
        count(distinct user_id) as visits_static
    from klaviyo.public.bi_website_visitors 
    where visitor_type = 'Visitor'
    and visit_type = 'Net New Visitor'
    and page_classification = 'marketing site'
    and visit_date >= '2022-01-01'
    group by 1
    order by 1 desc)
    ,
    New_Logos as (
    select
        date_trunc('day', MOST_RECENT_ACQUISITION_DATE)::date as start_date
        ,CURRENT_GMV_BUCKET as GMV
        ,SUM(LOGO_NEW_COUNT) as new_Logos
    from klaviyo.public.static_subscriptions
    where MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
    and coalesce(self_serve_status, 'Sales-Enabled') = 'Self-Enabled' 
    group by 1,2
    order by 1 desc)
    ,
    -- Activated metrics combined
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
        date_trunc('day', la.min_activated_date)::date as earliest_activated_date
        -- won is the highest level of confidence dimension to segments, if it null, use initial_gmv_bucket
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , count(distinct af.klaviyo_account_id) as activated_static
    from klaviyo.sandbox.tbl_acquisition_funnel af
    join lead_aggregation la on af.bi_lead_id = la.bi_lead_id
    where earliest_activated_date >= '2022-01-01' 
    and GMV is not null
    and af.lead_type in ('Email', 'Both')
    group by 1,2
    order by 1 desc)
    ,
    Workable as (
    select 
        date_trunc('day', lead_workable_date)::date as workable_date
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , sum(workable_lead_flag) as workable_count_SE
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where workable_date >= '2022-01-01' 
    --Email--- 
    and lead_type in ('Email', 'Both') 
    --Inbound Only 
    and coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound' 
    --AE--
    and product_specialist_flag = 'Account Executive'
    group by 1,2
    order by 1)
    ,
    Flips as (
    --SE Inbound flips (SE default) AE Only --
    select 
        date_trunc('day', lead_flip_date)::date as flip_date
        , case when af.won_gmv_bucket is not null
            then af.won_gmv_bucket
            else af.initial_gmv_bucket end as GMV
        , sum(flipped_flag) as flip_count_SE
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where flip_date::date >= '2022-01-01' 
    --Inbound Only
    and  coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound'   
    --Email---
    and lead_type in ('Email', 'Both')
    --AE--
    and product_specialist_flag = 'Account Executive'
    group by 1,2
    order by 1
    )
    ,
    MRR_ASP as (--SS MRR_ASP
    select
        -- according to Evan Cover    
        date_trunc('day', MOST_RECENT_ACQUISITION_DATE) as start_date
        ,CURRENT_GMV_BUCKET as GMV
         --email asp numerator
        ,  sum(email_new_mrr) + sum(email_reactivation_mrr) + sum(email_additional_product_mrr) as mrr
         --email asp denominator
        , sum(email_logo_new_count)+ sum(email_logo_reactivation_count) + sum(email_logo_additional_product_count) as paying_customer_cnt
        , round(div0(MRR, paying_customer_cnt),2) as asp   
    from klaviyo.public.static_subscriptions
    where  1=1
    and coalesce(self_serve_status, 'Sales-Enabled') = 'Self-Enabled'
    and MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
    group by 1,2
    order by 1 desc)

    select M.date_time, 
            M.Date_level,
            M.Status,
            M.GMV,
            V.visits_static, 
            A.activated_static, 
            M.mqls, 
            M.signups, 
            M.demo_requests, 
            W.workable_count_SE,
            F.flip_count_SE,
            round(div0(M.mqls,V.visits_static),4)*100 as MQL_CVR,
            round(div0(M.signups,V.visits_static),4)*100 as Signup_CVR,
            round(div0(M.demo_requests,V.visits_static),4)*100 as DemoRequests_CVR,        
            L.new_Logos,
            P.paying_customer_cnt,
            P.mrr,
            P.asp
    from MQL_Signups_Demo M
    left join Visits V on M.date_time = V.date_time
    left join Activated_Final A on M.date_time = A.earliest_activated_date and M.GMV = A.GMV
    left join New_Logos L on L.start_date = M.date_time and M.GMV = L.GMV
    left join MRR_ASP P on M.date_time = P.start_date and M.GMV = P.GMV
    left join Workable W on M.date_time = W.workable_date and M.GMV = W.GMV
    left join Flips F on  M.date_time = F.flip_date and M.GMV = F.GMV)

    union all

    -----------------12.(SE&SS)_GMV_BRK_Daily----------------------------------
    select * from
    (with MQL_Signups_Demo as (
    select 
        date_trunc('day', af.lead_mql_date)::date as date_time
        ,'Daily' as Date_level
        , 'All' as Status
        -- won is the highest level of confidence dimension to segments, if it null, use initial_gmv_bucket
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , sum(case when af.lead_mql_date is not null and af.mql_flag = 1 then 1 else 0 end) as mqls
        , sum(case when af.klaviyo_account_created_date is not null then 1 else 0 end) as signups
        , sum(case when af.demo_request_date is not null then 1 else 0 end) as demo_requests                                                                          
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where af.lead_mql_date >= '2022-01-01' 
    -- and af.won_opportunity_self_serve_flag = FALSE 
    and GMV is not null
    group by 1, 2, 3, 4
    order by 1 desc, 5 desc)
    ,  --unique net new website visitors (marketing/site traffic only exclude in-app traffic), regardless of GMV segments and SS/SE status
    Visits as (
    select 
        date_trunc('day', visit_date)::date as date_time,
        count(distinct user_id) as visits_static
    from klaviyo.public.bi_website_visitors 
    where visitor_type = 'Visitor'
    and visit_type = 'Net New Visitor'
    and page_classification = 'marketing site'
    and visit_date >= '2022-01-01'
    group by 1
    order by 1 desc)
    ,
    New_Logos as (
    select
        date_trunc('day', MOST_RECENT_ACQUISITION_DATE)::date as start_date
        ,CURRENT_GMV_BUCKET as GMV
        ,SUM(LOGO_NEW_COUNT) as new_Logos
    from klaviyo.public.static_subscriptions
    where 1=1
    -- and coalesce(self_serve_status, 'Sales-Enabled') = 'Self-Enabled' 
    and MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
    group by 1,2
    order by 1 desc)
    ,
    -- Activated metrics combined
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
        date_trunc('day', la.min_activated_date)::date as earliest_activated_date
        -- won is the highest level of confidence dimension to segments, if it null, use initial_gmv_bucket
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , count(distinct af.klaviyo_account_id) as activated_static
    from klaviyo.sandbox.tbl_acquisition_funnel af
    join lead_aggregation la on af.bi_lead_id = la.bi_lead_id
    where earliest_activated_date >= '2022-01-01' 
    and GMV is not null
    and af.lead_type in ('Email', 'Both')
    group by 1,2
    order by 1 desc)
    ,
    Workable as (
    select 
        date_trunc('day', lead_workable_date)::date as workable_date
        , case when af.won_gmv_bucket is not null
                then af.won_gmv_bucket
                else af.initial_gmv_bucket end as GMV
        , sum(workable_lead_flag) as workable_count_SE
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where workable_date >= '2022-01-01' 
    --Email--- 
    and lead_type in ('Email', 'Both') 
    --Inbound Only 
    and coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound' 
    --AE--
    and product_specialist_flag = 'Account Executive'
    group by 1,2
    order by 1)
    ,
    Flips as (
    --SE Inbound flips (SE default) AE Only --
    select 
        date_trunc('day', lead_flip_date)::date as flip_date
        , case when af.won_gmv_bucket is not null
            then af.won_gmv_bucket
            else af.initial_gmv_bucket end as GMV
        , sum(flipped_flag) as flip_count_SE
    from klaviyo.sandbox.tbl_acquisition_funnel af
    where flip_date::date >= '2022-01-01' 
    --Inbound Only
    and coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound'   
    --Email---
    and lead_type in ('Email', 'Both')
    --AE--
    and product_specialist_flag = 'Account Executive'
    group by 1,2
    order by 1
    )
    ,
    MRR_ASP as (--SS MRR_ASP
    select
        -- according to Evan Cover    
        date_trunc('day', MOST_RECENT_ACQUISITION_DATE) as start_date
        ,CURRENT_GMV_BUCKET as GMV
         --email asp numerator
        ,  sum(email_new_mrr) + sum(email_reactivation_mrr) + sum(email_additional_product_mrr) as mrr
         --email asp denominator
        , sum(email_logo_new_count)+ sum(email_logo_reactivation_count) + sum(email_logo_additional_product_count) as paying_customer_cnt
        , round(div0(MRR, paying_customer_cnt),2) as asp   
    from klaviyo.public.static_subscriptions
    where 1=1
    -- and coalesce(self_serve_status, 'Sales-Enabled') = 'Self-Enabled'
    and MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
    group by 1,2
    order by 1 desc)

    select M.date_time, 
            M.Date_level,
            M.Status,
            M.GMV,
            V.visits_static, 
            A.activated_static, 
            M.mqls, 
            M.signups, 
            M.demo_requests, 
            W.workable_count_SE,
            F.flip_count_SE,
            round(div0(M.mqls,V.visits_static),4)*100 as MQL_CVR,
            round(div0(M.signups,V.visits_static),4)*100 as Signup_CVR,
            round(div0(M.demo_requests,V.visits_static),4)*100 as DemoRequests_CVR,        
            L.new_Logos,
            P.paying_customer_cnt,
            P.mrr,
            P.asp
    from MQL_Signups_Demo M
    left join Visits V on M.date_time = V.date_time
    left join Activated_Final A on M.date_time = A.earliest_activated_date and M.GMV = A.GMV
    left join New_Logos L on L.start_date = M.date_time and M.GMV = L.GMV
    left join MRR_ASP P on M.date_time = P.start_date and M.GMV = P.GMV
    left join Workable W on M.date_time = W.workable_date and M.GMV = W.GMV
    left join Flips F on  M.date_time = F.flip_date and M.GMV = F.GMV)
    )
order by 1 desc,2 desc