SELECT

	  `campaigns_id`
	, `account_id`
	, `dates_id`
	, `id`
	, `impressions_search`
	, `clicks_search`
	, `cost_search`
	, `impressions_context`
	, `clicks_context`
	, `cost_context`

FROM {{ source('mybi', 'direct_campaigns_facts') }}