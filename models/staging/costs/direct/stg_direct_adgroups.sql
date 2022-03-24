SELECT

	  `id`
	, toUInt64OrNull(`adgroup_id`) AS `adgroup_id`
	, `name`

FROM {{ source('mybi', 'direct_adgroups') }}