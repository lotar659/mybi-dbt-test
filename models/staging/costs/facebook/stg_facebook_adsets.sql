SELECT

	  JSONExtract(_airbyte_data, 'id', 'UInt32') AS id
	, toUInt64OrNull(JSONExtract(_airbyte_data, 'adset_id', 'String')) AS adset_id
	, JSONExtract(_airbyte_data, 'name', 'String') AS name

FROM {{ source('mybi', 'facebook_adsets') }}