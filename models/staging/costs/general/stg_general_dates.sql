SELECT

	  `id`
	, `simple_date`

FROM {{ source('mybi', 'general_dates') }}