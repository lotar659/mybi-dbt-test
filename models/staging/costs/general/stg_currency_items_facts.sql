SELECT

	  `dates_id`
	, `items_id`
	, `id`
	, `account_id`
	, `rate`

FROM {{ source('mybi', 'currency_items_facts') }}