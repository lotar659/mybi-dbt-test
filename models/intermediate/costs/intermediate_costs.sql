SELECT

	  account_id
	, caption 
	
	, company	    
	  
    , coalesce(nullif(source, ''), 'undefined') as source
    , coalesce(nullif(medium, ''), 'undefined') as medium
  	, coalesce(nullif(campaign, ''), 'undefined') as campaign
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
	  
    , coalesce(nullif(source, ''), 'undefined') as source
    , coalesce(nullif(medium, ''), 'undefined') as medium
  	, coalesce(nullif(campaign, ''), 'undefined') as campaign
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
	  
    , coalesce(nullif(source, ''), 'undefined') as source
    , coalesce(nullif(medium, ''), 'undefined') as medium
  	, coalesce(nullif(campaign, ''), 'undefined') as campaign
	, campaign_id
	
	, dt
	  
	, impressions
	, clicks
	, cost
	  
FROM {{ ref('intermediate_facebook') }}
