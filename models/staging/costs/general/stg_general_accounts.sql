SELECT

	  JSONExtract(_airbyte_data, 'id', 'UInt16') AS account_id
	, JSONExtract(_airbyte_data, 'caption', 'String') AS caption

FROM {{ source('mybi', 'general_accounts') }}