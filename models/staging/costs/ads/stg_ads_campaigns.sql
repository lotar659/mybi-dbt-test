SELECT

	  JSONExtract(_airbyte_data, 'id', 'UInt32') AS id
	, JSONExtract(_airbyte_data, 'campaign_id', 'UInt64') AS campaign_id
	, JSONExtract(_airbyte_data, 'name', 'String') AS name

FROM {{ source('mybi', 'ads_campaigns') }}