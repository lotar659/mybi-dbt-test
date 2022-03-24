SELECT

	  cf.account_id as account_id
	, ga.caption as caption
	
	, CASE
		WHEN cf.account_id IN (35966) THEN 'zazumedia'
		WHEN cf.account_id IN (39753) THEN 'smsdar'
	  END AS company	    
	  
	, 'cpc' as medium
	, 'facebook' as source  
  	, cp.name AS campaign
	, CAST(cp.campaign_id AS UInt32) AS campaign_id
	
	, gd.simple_date AS dt
	  
	, cf.impressions as impressions
	, cf.clicks as clicks
	, cf.cost * coalesce(cif1.rate, 1) * {{ var("tax_multiplier_facebook") }} as cost
	  
FROM {{ ref('stg_facebook_campaigns_facts') }} AS cf
	LEFT JOIN {{ ref('stg_facebook_campaigns') }} AS cp
		ON cp.id = cf.campaigns_id 
	LEFT JOIN {{ ref('stg_general_dates') }} AS gd
		ON gd.id = cf.dates_id 
	LEFT JOIN {{ ref('stg_general_accounts') }} AS ga
		ON ga.account_id = cf.account_id
	LEFT JOIN {{ ref('stg_currency_items_facts') }} AS cif1 
		ON cif1.dates_id = cf.dates_id 
			AND cif1.items_id = CAST(22 AS Int32)

settings join_use_nulls = 1