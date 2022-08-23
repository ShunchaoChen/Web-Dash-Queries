-- SE Funnel

with WORKABLES as (
--SE Inbound workables (SE default) AE Only --
select 
date_trunc('week', lead_workable_date)::date as week_date 
, sum(workable_lead_flag) as workable_count 
from klaviyo.sandbox.tbl_acquisition_funnel 
where week_date >= '2022-01-01' 
--Email--- 
and lead_type in ('Email', 'Both') 
--Inbound Only 
and coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound' 
--AE--
and product_specialist_flag = 'Account Executive'
group by 1
order by 1 desc), FLIPS as (
--SE Inbound flips (SE default) AE Only --
select 
date_trunc('week', lead_flip_date)::date as flip_date
, sum(flipped_flag) as flip_count
from klaviyo.sandbox.tbl_acquisition_funnel 
where flip_date::date >= '2022-01-01'
--Inbound Only
and  coalesce(won_aggregated_lead_source, opp_aggregated_lead_source, initial_aggregated_lead_source) = 'Inbound'   
--Email---
and lead_type in ('Email', 'Both')
--AE--
and product_specialist_flag = 'Account Executive'
group by 1
order by 1 desc), OPPORTUNITY as (
--SE Inbound Opportunities AE Only --                 
select 
date_trunc('week', opportunity_created_date)::date as opportunity_created_date
, sum(opportunity_flag) as opp_count
, sum(opportunity_amount) as new_pipeline_amount
from klaviyo.sandbox.tbl_acquisition_funnel 
where opportunity_created_date::date >= '2022-01-01'
--Inbound Only
and  opp_aggregated_lead_source = 'Inbound'   
--SE--
and coalesce(won_opportunity_self_serve_flag, FALSE) = FALSE 
--Email---
and lead_type in ('Email', 'Both')
--AE--
and product_specialist_flag = 'Account Executive'
group by 1
order by 1 desc
), MRR_ASP as (--SE MRR_ASP
select
-- according to Evan Cover    
date_trunc('week', MOST_RECENT_ACQUISITION_DATE) as start_date
 --email asp numerator
,  sum(email_new_mrr) + sum(email_reactivation_mrr) + sum(email_additional_product_mrr) as Closed_won_amount
 --email asp denominator
, sum(email_logo_new_count)+ sum(email_logo_reactivation_count) + sum(email_logo_additional_product_count) as paying_customer_cnt
, round(div0(Closed_won_amount, paying_customer_cnt),2) as asp   
from klaviyo.public.static_subscriptions
where  coalesce(self_serve_status, 'Sales-Enabled') = 'Sales-Enabled' 
and MOST_RECENT_ACQUISITION_DATE >= '2022-01-01'
group by 1
order by 1 desc)

select W.week_date, 
        W.workable_count,
        F.flip_count, 
        O.opp_count,
        P.Closed_won_amount,
        P.paying_customer_cnt,
        P.asp
from WORKABLES W
left join FLIPS F on W.week_date = F.flip_date
left join OPPORTUNITY O on O.opportunity_created_date = W.week_date
left join MRR_ASP P on P.start_date = W.week_date;