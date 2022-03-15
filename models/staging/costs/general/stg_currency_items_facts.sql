SELECT

	  JSONExtract(_airbyte_data, 'dates_id', 'UInt32') AS dates_id
	, JSONExtract(_airbyte_data, 'items_id', 'UInt8') AS items_id
	, JSONExtract(_airbyte_data, 'id', 'UInt32') AS id
	, JSONExtract(_airbyte_data, 'account_id', 'UInt16') AS account_id
	, JSONExtract(_airbyte_data, 'rate', 'Float32') AS rate

FROM {{ source('mybi', 'currency_items_facts') }}