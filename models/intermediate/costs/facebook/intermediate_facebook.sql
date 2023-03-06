    
	  

SELECT

	  cf.account_id as account_id
	, CAST(halfMD5(CONCAT(CAST(cf.id, 'String'), 'facebook')), 'UInt64') as id


	, ga.caption as caption
	, CASE
		WHEN cf.account_id IN (35966) THEN 'zazumedia'
		WHEN cf.account_id IN (39753) THEN 'smsdar'
	  END AS company	    

	, gd.simple_date AS dt
    , 'undefined' as device
    , 'ad' as traffic_source    
	, 'facebook' as source  
    --, gt.medium as traffic_medium
	, 'cpc' as medium
	, CAST(cp.campaign_id AS UInt32) AS campaign_id
  	, coalesce(cp.name, 'undefined') as campaign

    , gt.campaign as traffic_campaign
    , gt.content as content
    , gt.keyword as keyword
    , gs.domain as domain
    , concat('https://', gs.domain, gt.landing_page) as landing_page

    , gt.region_id as region_id
	
	, cf.impressions as impressions
	, cf.clicks as clicks
	, CAST((cf.cost * coalesce(cif1.rate, 1) * {{ var("tax_multiplier_facebook") }}) AS Float64) as cost

FROM {{ ref('stg_facebook_ads_facts') }} AS cf
	LEFT JOIN {{ ref('stg_general_accounts') }} AS ga
		ON ga.account_id = cf.account_id
	LEFT JOIN {{ ref('stg_general_dates') }} AS gd
		ON gd.id = cf.dates_id 
	LEFT JOIN {{ ref('stg_general_sites') }} AS gs
		ON gs.id = cf.sites_id
	LEFT JOIN {{ ref('stg_general_traffic') }} AS gt
		ON gt.id = cf.traffic_id        
	LEFT JOIN {{ ref('stg_facebook_campaigns') }} AS cp
		ON cp.id = cf.campaigns_id 
	LEFT JOIN {{ ref('stg_currency_items_facts') }} AS cif1 
		ON cif1.dates_id = cf.dates_id 
			AND cif1.items_id = CAST(22 AS Int32)

settings join_use_nulls = 0