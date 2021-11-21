SELECT
	cf.id AS id

--	, cf.dates_id
	, parseDateTimeBestEffort(dt.simple_date) AS dt
	
--	, cf.sites_id
	, st."domain" AS site_domain 
	, st.description AS site_description
	
	, cf.account_id AS account_id
	, ac.status as account_status
	, ac.caption as account_caption
	, ac.enabled as account_enabled
	, ac.service as account_service
--	, ac.account_id AS internal_account_id
	
	, cf.traffic_id as traffic_id
	, tr."source" as traffic_source
	, tr.medium as traffic_medium
	, tr.campaign as traffic_campaign
	, tr."content" as traffic_content
	, tr.keyword as traffic_keyword
	, tr."grouping" as traffic_grouping
	, tr.landing_page as traffic_landing_page
		
	, vat_included
	, impressions	
	, clicks	
	, "cost"
	
FROM mybi.general_costs_facts AS cf
	LEFT JOIN mybi.general_dates AS dt ON cf.dates_id = dt.id 
	LEFT JOIN mybi.general_sites AS st ON cf.sites_id = st.id
	LEFT JOIN mybi.general_accounts AS ac ON cf.account_id = ac.account_id 
	LEFT JOIN mybi.general_traffic AS tr ON cf.traffic_id = tr.id 
