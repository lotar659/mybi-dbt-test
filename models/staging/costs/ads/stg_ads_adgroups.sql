SELECT

	  JSONExtract(_airbyte_data, 'id', 'UInt32') AS id
	, JSONExtract(_airbyte_data, 'adgroup_id', 'UInt64') AS adgroup_id
	, JSONExtract(_airbyte_data, 'name', 'String') AS name

FROM {{ source('mybi', 'ads_adgroups') }}