-- YANDEX SEARCH
SELECT
	
	cf.account_id as account_id

	, ga.caption as caption
	, CASE
		WHEN cf.account_id IN (35956) THEN 'zazumedia'
		WHEN cf.account_id IN (35959) THEN 'smsdar'
		WHEN cf.account_id IN (36228) THEN 'smsdar'
		WHEN cf.account_id IN (35962) THEN 'smspobeda'
	END AS company	  
	
	, gd.simple_date AS dt
    , cf.device as device
    --, gt.source as traffic_source
	, 'yandex.search' as source  
    , gt.medium as medium
	, CAST(cp.campaign_id AS UInt32) AS campaign_id
	, cp.name AS campaign
    , gt.campaign as traffic_campaign
    , gt.content as content
    , gt.keyword as keyword
    , gs.domain as domain
    , gt.landing_page as landing_page    
	
	, cf.impressions_search as impressions
	, cf.clicks_search as clicks
	, cf.cost_search as cost
	
FROM {{ ref('stg_direct_ads_facts') }} AS cf
	LEFT JOIN {{ ref('stg_general_accounts') }} AS ga
		ON ga.account_id = cf.account_id
	LEFT JOIN {{ ref('stg_general_dates') }} AS gd
		ON gd.id = cf.dates_id 
	LEFT JOIN {{ ref('stg_direct_campaigns') }} AS cp
		ON cp.id = cf.campaigns_id 
	LEFT JOIN {{ ref('stg_general_sites') }} AS gs
		ON gs.id = cf.sites_id
	LEFT JOIN {{ ref('stg_general_traffic') }} AS gt
		ON gt.id = cf.traffic_id

settings join_use_nulls = 1		