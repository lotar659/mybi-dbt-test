select

	  CAST(id, 'UInt64') as id
    , account_id
	, caption
	, company
	, dt
    , 'ad' as traffic_source
    , device
	  
    , coalesce(source, 'undefined') as source
    , coalesce(medium, 'undefined') as medium
	, campaign_id
  	, coalesce(campaign, 'undefined') as campaign
    , traffic_campaign
    , content
    , keyword
    , concat('https://', domain, landing_page) as landing_page
    , region_id
	  
	, impressions
	, clicks
	, CAST(cost AS Float64) as cost

from {{ ref('intermediate_direct_context') }}

union all

select

	  CAST(id, 'UInt64') as id
    , account_id
	, caption
	, company
	, dt
    , 'ad' as traffic_source
    , device
	  
    , coalesce(source, 'undefined') as source
    , coalesce(medium, 'undefined') as medium
	, campaign_id
  	, coalesce(campaign, 'undefined') as campaign
    , traffic_campaign
    , content
    , keyword
    , concat('https://', domain, landing_page) as landing_page
    , region_id
	  
	, impressions
	, clicks
	, CAST(cost AS Float64) as cost

from {{ ref('intermediate_direct_search') }}
