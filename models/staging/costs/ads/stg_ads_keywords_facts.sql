SELECT

	  `account_id`
	, `id`
	, `dates_id`
    , `traffic_id`
	, `sites_id`

	, `campaigns_id`
	, `adgroups_id`
	, `keywords_id`

	, `ad_network`
	, `device`

	, `impressions`
	, `clicks`
	, `cost`

FROM {{ source('mybi', 'ads_keywords_facts') }}