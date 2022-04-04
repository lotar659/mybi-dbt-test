SELECT

	  account_id
	, caption 
	, company	    
    , dt
    , 'ad' as traffic_source
    , device
	  
    , coalesce(source, 'undefined') as source
    , coalesce(medium, 'undefined') as medium
	, campaign_id
  	, coalesce(campaign, 'undefined') as campaign
    , traffic_campaign
    , content
    , keyword
    , concat('https://', domain, landing_page) as landing_page
	
	, impressions
	, clicks
	, CAST(cost AS Float64) as cost
	  
FROM {{ ref('intermediate_ads') }} 

UNION ALL

SELECT

	  account_id
	, caption
	, company
	, dt
    , 'ad' as traffic_source
    , device
	  
    , coalesce(source, 'undefined') as source
    , coalesce(medium, 'undefined') as medium
	, campaign_id
  	, coalesce(campaign, 'undefined') as campaign
    , traffic_campaign
    , content
    , keyword
    , concat('https://', domain, landing_page) as landing_page
	  
	, impressions
	, clicks
	, CAST(cost AS Float64) as cost
	  
FROM {{ ref('intermediate_direct') }} 

UNION ALL

SELECT

	  account_id
	, caption
	, company
	, dt
    , 'ad' as traffic_source    
    , device
	  
    , coalesce(source, 'undefined') as source
    , coalesce(medium, 'undefined') as medium
	, campaign_id
  	, coalesce(campaign, 'undefined') as campaign
    , traffic_campaign
    , content
    , keyword
    , concat('https://', domain, landing_page) as landing_page
	  
	, impressions
	, clicks
	, CAST(cost AS Float64) as cost
	  
FROM {{ ref('intermediate_facebook') }}

-- + costs from 'organic' channel attributed manually in gSheet: https://docs.google.com/spreadsheets/d/1dQLDrUuB_MfBtXX8LxqbgzZxnigdPkBXw-s6GnnQwBc/edit#gid=1017165270
UNION ALL

SELECT

	  0 as account_id
	, '' as caption
	, company
	, date_month as dt
	, 'organic' as traffic_source    
    , '' as device
	  
    , '' as source
    , '' as medium
	, 0 as campaign_id
  	, '' as campaign
    , '' as traffic_campaign
    , '' as content
    , '' as keyword
    , '' as landing_page
	  
	, 0
	, 0
	, cost
	  
FROM {{ source('gsheet', 'organic_costs') }}