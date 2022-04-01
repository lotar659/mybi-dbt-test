select

	  account_id
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

	, impressions
	, clicks
	, cost

from {{ ref('intermediate_ads_ads') }}

union all

select

	  account_id
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

	, impressions
	, clicks
	, cost

from {{ ref('intermediate_ads_keywords') }}
