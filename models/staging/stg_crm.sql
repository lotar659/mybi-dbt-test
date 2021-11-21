SELECT

	  lf.id as id

	, lf.clientids_id as client_id
	, cl.email as client_email
	, cl.phone as client_phone
	, cl.clientid as client_amo_id

	, lf.users_id as "user_id"
	, us."name"  as "user_name"
	, us.group_name  as "user_group_name"
	
	, lf.leads_id as lead_id
	, ld."name"  as lead_name
	, ld.status  as lead_status
	, ld.pipeline  as lead_pipeline
	, ld.loss_reason  as lead_loss_reason
	, ld.is_deleted  as lead_is_deleted

	, lf.contacts_id	as contact_id
	, ct."name" as contact_name
	, ct.email as contact_email
	, ct.phone as contact_phone
	
	, lf.companies_id as company_id
	, cp."name"  as company_name
	, cp.email  as company_email
	, cp.phone  as company_phone
	
--	, lf.created_id
	, parseDateTimeBestEffort(dt_created.simple_date) AS dt_created
	
--	, lf.closed_id
	, parseDateTimeBestEffort(dt_closed.simple_date) AS dt_closed
	
	, lf.account_id as account_id
	, ac.status  as account_status
	, ac.caption  as account_caption
	, ac.enabled  as account_enabled
	, ac.service 	 as account_service

	, lf.traffic_id as traffic_id
	, tr."source"  as traffic_source
	, tr.medium as traffic_medium
	, tr.campaign  as traffic_campaign
	, tr."content"  as traffic_content
	, tr.keyword  as traffic_keyword
	, tr."grouping"  as traffic_grouping
	, tr.landing_page  as traffic_landing_page
	
	, lf.price
	
FROM mybi.amocrm_leads_facts AS lf
	LEFT JOIN mybi.general_clientids AS cl ON cl.id = lf.clientids_id 
	LEFT JOIN mybi.amocrm_users AS us ON us.id = lf.users_id 	
	LEFT JOIN mybi.amocrm_leads AS ld ON ld.id = lf.leads_id 	
	LEFT JOIN mybi.amocrm_contacts AS ct ON ct.id = lf.contacts_id 	
	LEFT JOIN mybi.amocrm_companies AS cp ON cp.id = lf.companies_id 	
	LEFT JOIN mybi.general_dates AS dt_created ON dt_created.id = lf.created_id 	
	LEFT JOIN mybi.general_dates AS dt_closed ON dt_closed.id = lf.closed_id	
	LEFT JOIN mybi.general_accounts AS ac ON ac.account_id = lf.account_id 
	LEFT JOIN mybi.general_traffic AS tr ON tr.id = lf.traffic_id 
