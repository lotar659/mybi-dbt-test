select

	  account_id
	, caption
	, company	  
	, medium
	, source
	, campaign
	, campaign_id
	, dt
	, impressions
	, clicks
	, cost

from {{ ref('intermediate_direct_context') }}

union all

select

	  account_id
	, caption
	, company	  
	, medium
	, source
	, campaign
	, campaign_id
	, dt
	, impressions
	, clicks
	, cost

from {{ ref('intermediate_direct_search') }}
