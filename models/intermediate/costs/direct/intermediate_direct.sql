select

      id
	, account_id
	, caption
	, company	  
	, dt
    , device

	, source
	, medium
	, campaign_id
	, campaign
    , traffic_campaign
    , content
    , keyword
    , domain
    , landing_page
    , region_id

	, impressions
	, clicks
	, cost

from {{ ref('intermediate_direct_context') }}

union all

select

      id
	, account_id
	, caption
	, company	  
	, dt
    , device

	, source
	, medium
	, campaign_id
	, campaign
    , traffic_campaign
    , content
    , keyword
    , domain
    , landing_page
    , region_id

	, impressions
	, clicks
	, cost

from {{ ref('intermediate_direct_search') }}
