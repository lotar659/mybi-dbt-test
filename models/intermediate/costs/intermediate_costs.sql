SELECT

	  account_id
	, caption 
	, company	    
    , dt
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
