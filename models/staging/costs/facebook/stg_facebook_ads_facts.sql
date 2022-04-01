SELECT

	  `account_id`
	, `id`
	, `dates_id`
    , `traffic_id`
    , `sites_id`

	, `campaigns_id`
	, `adsets_id`
	, `ads_id`

	, `impressions`
	, `clicks`
	, `cost`

FROM {{ source('mybi', 'facebook_ads_facts') }}
