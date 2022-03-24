SELECT

	  `id`
	, toUInt64OrNull(`ad_id`) AS `ad_id`
	, `name`

FROM {{ source('mybi', 'facebook_ads') }}