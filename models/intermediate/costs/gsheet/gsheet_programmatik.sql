SELECT

      halfMD5(CONCAT(CAST(date_month, 'String'), company, 'programmatik')) as id      
	, 0 as account_id
	, '' as caption
	, company
	, date_month as dt
	, 'ad' as traffic_source    
    , 'undefined' as device

    , 'getintent' as source
    , '' as medium
	, 0 as campaign_id
  	, '' as campaign
    , '' as traffic_campaign
    , '' as content
    , '' as keyword
    , '' as landing_page
    , 0 as region_id
	  
	, 0 as impressions
	, 0 as clicks
	, CAST(cost, 'Float64') as cost
	  
FROM {{ source('gsheet', 'programmatik') }}