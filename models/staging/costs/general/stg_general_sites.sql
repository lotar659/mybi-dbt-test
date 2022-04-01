SELECT

	id
	, `domain`
	, `description`

FROM {{ source('mybi', 'general_sites') }}
