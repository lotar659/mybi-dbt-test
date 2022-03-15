SELECT

	  JSONExtract(_airbyte_data, 'campaigns_id', 'UInt32') AS campaigns_id
	, JSONExtract(_airbyte_data, 'account_id', 'UInt16') AS account_id
	, JSONExtract(_airbyte_data, 'dates_id', 'UInt32') AS dates_id
	, JSONExtract(_airbyte_data, 'id', 'UInt32') AS id

	, JSONExtract(_airbyte_data, 'impressions_search', 'UInt32') AS impressions_search
	, JSONExtract(_airbyte_data, 'clicks_search', 'UInt32') AS clicks_search
	, JSONExtract(_airbyte_data, 'cost_search', 'Float32') AS cost_search

	, JSONExtract(_airbyte_data, 'impressions_context', 'UInt32') AS impressions_context
	, JSONExtract(_airbyte_data, 'clicks_context', 'UInt32') AS clicks_context
	, JSONExtract(_airbyte_data, 'cost_context', 'Float32') AS cost_context

FROM {{ source('mybi', 'direct_campaigns_facts') }}