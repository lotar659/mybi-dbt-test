SELECT

	  `account_id`
	, `dates_id`
	, `id`
	, `campaigns_id`
	, `adgroups_id`
	, `keywords_id`
	, `impressions`
	, `clicks`
	, `cost`

FROM {{ source('mybi', 'ads_keywords_facts') }}