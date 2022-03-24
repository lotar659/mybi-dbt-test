SELECT

	  `id`
	, CAST(`campaign_id` AS UInt64) as `campaign_id`
	, `name`

FROM {{ source('mybi', 'ads_campaigns') }}