select

	-- common
      id as row_id
	, company
    , row_source      
    , create_date

	, if(traffic_source = '', 'undefined', traffic_source) as traffic_source
	, if(`source` = '', 'undefined', `source`) as `source`
	, if(medium = '', 'undefined', medium) as medium
	, if(campaign = '', 'undefined', campaign) as campaign
	, if(content = '', 'undefined', content) as content

	, if(keyword = '', 'undefined', keyword) as keyword
	, if(landing_page = '', 'undefined', landing_page) as landing_page
	, if(device = '', 'undefined', device) as device
	, if(region = '', 'undefined', region) as region

	-- costs
	, impressions
	, clicks
	, cost

	-- visits
	, client_id

	-- amocrm
	, lead_id
	, date_payment
	, reason_date
	, date_close

	, responsible_user
	, status
	, pipeline
    , type
    , format
    , form
    , if(sphere = '', 'undefined', sphere) as sphere
    , is_blog
	
    , price

	-- margin rate
	, margin_rate

FROM {{ ref('intermediate_tracker') }} as tracker
	left join {{ source('gsheet', 'margin_rate') }} as margin_rate on margin_rate.date_month = date_trunc('month', tracker.create_date)
        and margin_rate.company = tracker.company