SELECT

	  JSONExtract(_airbyte_data, 'id', 'UInt32') AS id
	, toUInt64OrNull(JSONExtract(_airbyte_data, 'campaign_id', 'String')) AS campaign_id
	, JSONExtract(_airbyte_data, 'name', 'String') AS name

FROM {{ source('mybi', 'direct_campaigns') }}