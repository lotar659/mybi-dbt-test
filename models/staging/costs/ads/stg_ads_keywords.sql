SELECT

	  `id`
	, CAST(`keyword_id` AS UInt64) AS `keyword_id`
	, `name`

FROM {{ source('mybi', 'ads_keywords') }}