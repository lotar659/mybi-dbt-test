SELECT

	  account_id
	, caption 
	
	, company	    
	  
    , coalesce(source, 'undefined') as source
    , coalesce(medium, 'undefined') as medium
  	, coalesce(campaign, 'undefined') as campaign
	, campaign_id
	
	, dt
	  
	, impressions
	, clicks
	, cost
	  
FROM {{ ref('intermediate_ads') }} 

UNION ALL

SELECT

	  account_id
	, caption 
	
	, company	    
	  
    , coalesce(source, 'undefined') as source
    , coalesce(medium, 'undefined') as medium
  	, coalesce(campaign, 'undefined') as campaign
	, campaign_id
	
	, dt
	  
	, impressions
	, clicks
	, cost
	  
FROM {{ ref('intermediate_direct') }} 

UNION ALL

SELECT

	  account_id
	, caption 
	
	, company	    
	  
    , coalesce(source, 'undefined') as source
    , coalesce(medium, 'undefined') as medium
  	, coalesce(campaign, 'undefined') as campaign
	, campaign_id
	
	, dt
	  
	, impressions
	, clicks
	, cost
	  
FROM {{ ref('intermediate_facebook') }}
