SELECT

	  `id`
	, toUInt64OrNull(`campaign_id`) AS `campaign_id`
	, `name`

FROM {{ source('mybi', 'direct_campaigns') }}