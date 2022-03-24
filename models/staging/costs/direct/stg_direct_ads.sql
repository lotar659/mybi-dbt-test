SELECT

	  `id`
	, toUInt64OrNull(`ad_id`) AS `ad_id`
	, `title` as `name`

FROM {{ source('mybi', 'direct_ads') }}