SELECT

	  `account_id`
	, `dates_id`
	, `id`
	, `campaigns_id`
	, `adsets_id`
	, `ads_id`
	, `impressions`
	, `clicks`
	, `cost`

FROM {{ source('mybi', 'facebook_ads_facts') }}