SELECT

	id
	, `source`
	, medium
	, campaign
	, content
	, keyword
	, traffic_hash
	, `grouping`
	, landing_page

FROM {{ source('mybi', 'general_traffic') }}
