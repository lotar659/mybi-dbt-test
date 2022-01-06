SELECT

	  JSONExtract(_airbyte_data, 'account_id', 'UInt16') AS account_id
	, JSONExtract(_airbyte_data, 'dates_id', 'UInt32') AS dates_id
	  
	, JSONExtract(_airbyte_data, 'campaigns_id', 'UInt32') AS campaigns_id
	, JSONExtract(_airbyte_data, 'adgroups_id', 'UInt32') AS adgroups_id
	, JSONExtract(_airbyte_data, 'ads_id', 'UInt32') AS ads_id

	, JSONExtract(_airbyte_data, 'impressions', 'UInt32') AS impressions
	, JSONExtract(_airbyte_data, 'clicks', 'UInt32') AS clicks
	, JSONExtract(_airbyte_data, 'cost', 'Float32') AS cost

FROM {{ source('mybi', 'ads_ads_facts') }}