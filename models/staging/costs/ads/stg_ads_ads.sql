SELECT

	  JSONExtract(_airbyte_data, 'id', 'UInt32') AS id
	, JSONExtract(_airbyte_data, 'ad_id', 'UInt64') AS ad_id
	, JSONExtract(_airbyte_data, 'headline_part_one', 'String') AS name

FROM {{ source('mybi', 'ads_ads') }}