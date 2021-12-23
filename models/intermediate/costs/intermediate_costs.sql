SELECT

	  account_id
	, caption 
	
	, company	    
	  
    , source
    , medium  
  	, campaign
	, campaign_id
	
	, dt
	  
	, impressions
	, clicks
	, cost
	  
FROM {{ ref('intermediate_ads') }} 

union all 

SELECT

	  account_id
	, caption 
	
	, company	    
	  
    , source
    , medium  
  	, campaign
	, campaign_id
	
	, dt
	  
	, impressions
	, clicks
	, cost
	  
FROM {{ ref('intermediate_direct') }} 

union all

SELECT

	  account_id
	, caption 
	
	, company	    
	  
    , source
    , medium  
  	, campaign
	, campaign_id
	
	, dt
	  
	, impressions
	, clicks
	, cost
	  
FROM {{ ref('intermediate_facebook') }}
