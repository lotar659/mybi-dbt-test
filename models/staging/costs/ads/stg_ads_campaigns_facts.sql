SELECT

	  `campaigns_id`
	, `account_id`
	, `dates_id`
	, `id`
	, `impressions`
	, `clicks`
	, `cost`

FROM {{ source('mybi', 'ads_campaigns_facts') }}