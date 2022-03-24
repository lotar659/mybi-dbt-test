SELECT

	  `id`
	, CAST(`ad_id` AS UInt64) AS `ad_id`
	, `headline_part_one` as `name`

FROM {{ source('mybi', 'ads_ads') }}
