SELECT

	  account_id
	, adv_engine 
	
	, campaign_id
	, campaign_name

	, adgroup_id
	, adgroup_name

	, ad_id
	, ad_name
	  
FROM {{ ref('intermediate_ads_mapping') }} 

UNION ALL

SELECT

	  account_id
	, adv_engine 
	
	, campaign_id
	, campaign_name

	, adgroup_id
	, adgroup_name

	, ad_id
	, ad_name
	  
FROM {{ ref('intermediate_direct_mapping') }} 

UNION ALL

SELECT

	  account_id
	, adv_engine 
	
	, campaign_id
	, campaign_name

	, adset_id
	, adset_name

	, ad_id
	, ad_name
	  
FROM {{ ref('intermediate_facebook_mapping') }} 
