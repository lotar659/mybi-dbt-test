select 

	  company
	, `source`
	, UTMCampaign
	, campaign_name
	, is_enabled

from {{ source('gsheet', 'campaign_mapping') }}