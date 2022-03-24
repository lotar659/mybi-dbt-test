SELECT

	  `id`
	, `caption`
	, `id` as `account_id`

FROM {{ source('mybi', 'general_accounts') }}