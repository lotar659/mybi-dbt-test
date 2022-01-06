SELECT

	  JSONExtract(_airbyte_data, 'id', 'UInt32') AS id
	, toUInt64OrNull(JSONExtract(_airbyte_data, 'ad_id', 'String')) AS ad_id
	, JSONExtract(_airbyte_data, 'title', 'String') AS name

FROM {{ source('mybi', 'direct_ads') }}