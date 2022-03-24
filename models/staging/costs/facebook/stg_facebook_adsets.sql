SELECT

	  `id`
	, toUInt64OrNull(`adset_id`) AS `adset_id`
	, `name`

FROM {{ source('mybi', 'facebook_adsets') }}