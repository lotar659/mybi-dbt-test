SELECT

	  JSONExtract(_airbyte_data, 'id', 'UInt32') AS id
	, JSONExtract(_airbyte_data, 'simple_date', 'String') AS simple_date

FROM {{ source('mybi', 'general_dates') }}