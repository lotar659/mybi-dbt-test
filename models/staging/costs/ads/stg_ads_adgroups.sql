SELECT

	  `id`
	, CAST(`adgroup_id` AS UInt64) as `adgroup_id`
	, `name`

FROM {{ source('mybi', 'ads_adgroups') }}
