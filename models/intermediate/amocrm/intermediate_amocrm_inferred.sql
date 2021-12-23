SELECT

	id
	, date_create
	, responsible_user
	, price
	, `source`
	, `type`
	, date_payment
	, UTM_Content

    , multiIf(
		notEmpty(UTM_Medium), UTM_Medium,
		notEmpty(iv.medium), iv.medium, 
		'') as utm_medium    

    , multiIf(
		notEmpty(UTM_Source), UTM_Source,
		notEmpty(source), source, 
		notEmpty(UTM_Source1), UTM_Source1,
		notEmpty(iv.source), iv.source, 
		'') as utm_source
	
    , multiIf(
		notEmpty(UTM_Campaign), UTM_Campaign,
		notEmpty(iv.campaign), iv.campaign, 
		'') as utm_campaign   	

	, UTM_Term
	, UTM_Stage
	, pipeline
	, status
	, format
	, city
	, reason
	, reason_client
	, href
	, gross
	, finance_source
	, client_id
	, reason_date
	, form
	, form_class
	, UTM_Source1
	, UTM_Placement
	, client_id_int
	, tags
	, date_close
	, UTM_retarget
	, UTM_phrase
	, UTM_field
	, UTM_referrer
	, type_payment
	, ga_utm
	, roistat
	, country
	, sphere
	, ad_name
	, referrer
	, openstat_service
	, openstat_campaign
	, openstat_ad
	, openstat_source
	, `from`
	, gclientid
	, gclid
	, yclid
	, fbclid
	, company

FROM {{ ref('stg_amocrm_united') }} AS ia
	LEFT ANY JOIN {{ ref('intermediate_visits') }} AS iv ON iv.client_id = ia.client_id_int 
