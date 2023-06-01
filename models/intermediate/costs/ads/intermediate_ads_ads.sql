SELECT

	  cf.account_id as account_id
    , halfMD5(CONCAT(CAST(cf.id, 'String'), 'stg_ads_ads_facts')) as id
    , gt.id as traffic_id

	, ga.caption as caption 
	, CASE
		WHEN cf.account_id IN (35957, 48415, 49079) THEN 'zazumedia'
		WHEN cf.account_id IN (35960, 35961, 47562) THEN 'smsdar'
		WHEN cf.account_id IN (35964) THEN 'smspobeda'
	  END AS company	  
	, gd.simple_date AS dt
    , cf.device as device
    , gt.source as source
	--, 'google' as source
    , gt.medium as medium
	, cp.campaign_id as campaign_id
  	, cp.name as campaign
    , gt.campaign as traffic_campaign
    , gt.content as content
    , gt.keyword as keyword
    , gs.domain as domain
    , gt.landing_page as landing_page
    , gt.region_id as region_id
	  
	, cf.impressions as impressions
	, cf.clicks as clicks
	, CAST(cf.cost * coalesce(cif1.rate, 1) * {{ var("tax_multiplier_google") }} AS Decimal(18,2)) as cost 
	--, cf.cost as cost_raw
	--, cif1.rate as cif1_rate
	--, cif2.rate as cif2_rate
	
FROM {{ ref('stg_ads_ads_facts') }} AS cf
	LEFT JOIN {{ ref('stg_general_accounts') }} AS ga
		ON ga.account_id = cf.account_id 
	LEFT JOIN {{ ref('stg_general_dates') }} AS gd
		ON gd.id = cf.dates_id 
	LEFT JOIN {{ ref('stg_ads_campaigns') }} AS cp
		ON cp.id = cf.campaigns_id 
	LEFT JOIN {{ ref('stg_general_sites') }} AS gs
		ON gs.id = cf.sites_id
	LEFT JOIN {{ ref('stg_general_traffic') }} AS gt
		ON gt.id = cf.traffic_id
	LEFT JOIN {{ ref('stg_currency_items_facts') }} AS cif1 
        ON cif1.dates_id = cf.dates_id 
            AND cif1.items_id = CAST(22 AS Int32)
            AND cf.account_id IN (35957, 35964, 47562, 48415, 49079)

settings join_use_nulls = 0