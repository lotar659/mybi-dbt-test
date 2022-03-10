select

	-- common
	  company
    , row_source      
    , create_date

	, traffic_source
	, `source`
	, medium
	, campaign
	, content

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
	
    , price

	-- margin rate
	, margin_rate

FROM {{ ref('intermediate_tracker') }} as tracker
	left join {{ source('gsheet', 'margin_rate') }} as margin_rate on margin_rate.date_month = date_trunc('month', tracker.create_date)