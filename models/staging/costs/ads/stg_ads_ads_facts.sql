SELECT

	  `account_id`
	, `dates_id`
	, `id`

	, `campaigns_id`
	, `adgroups_id`
	, `ads_id`
	
    , `impressions`
	, `clicks`
	, `cost`

FROM {{ source('mybi', 'ads_ads_facts') }}
