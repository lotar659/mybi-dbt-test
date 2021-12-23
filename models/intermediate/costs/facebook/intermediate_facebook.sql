SELECT

	  cf.account_id as account_id
	, ga.caption as caption
	
	, CASE
		WHEN cf.account_id IN (35966) THEN 'zazumedia'
		WHEN cf.account_id IN (36077) THEN 'zazumedia'
	  END AS company	    
	  
	, 'cpc' as medium
	, 'facebook' as source  
  	, cp.name AS campaign
	, CAST(cp.campaign_id AS UInt32) AS campaign_id
	
	, parseDateTime32BestEffortOrNull(gd.simple_date) AS dt
	  
	, cf.impressions as impressions
	, cf.clicks as clicks
	, cf.cost as cost
	  
FROM mybi.facebook_campaigns_facts AS cf
	LEFT JOIN mybi.facebook_campaigns AS cp
		ON cp.id = cf.campaigns_id 
	LEFT JOIN mybi.general_dates AS gd
		ON gd.id = cf.dates_id 
	LEFT JOIN mybi.general_accounts AS ga
		ON ga.account_id = cf.account_id