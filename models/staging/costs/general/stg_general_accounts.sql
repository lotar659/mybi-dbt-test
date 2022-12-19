SELECT

	  `id`
	, `caption`
	, `account_id`

FROM {{ source('mybi', 'general_accounts') }}