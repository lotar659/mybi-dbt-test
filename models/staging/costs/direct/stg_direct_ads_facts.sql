SELECT

	  `account_id`
	, `dates_id`
	, `id`
	, `campaigns_id`
	, `adgroups_id`
	, `ads_id`
	, `impressions_search`
	, `clicks_search`
	, `cost_search`
	, `impressions_context`
	, `clicks_context`
	, `cost_context`

FROM {{ source('mybi', 'direct_ads_facts') }}