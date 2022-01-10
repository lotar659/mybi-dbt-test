with intermediate_visits as (

	select 

		  dt
		, ts
		, client_id
		
		, traffic_source	
		, `source`
		, medium
		, campaign
		, content

		--, traffic_source_importance
		, row_number() over (partition by client_id, dt order by traffic_source_importance asc) as rn	

	from {{ ref('intermediate_visits') }}
	where client_id not in (0)
	settings allow_experimental_window_functions = 1

)

SELECT

	  ia.id as id
	, ia.date_create as date_create
	, ia.responsible_user as responsible_user
	, ia.price as price
	, ia.`source` as `source_raw`
	, ia.`type` as `type`
	, ia.date_payment as date_payment
	, ia.UTM_Content as UTM_Content
	, ia.UTM_Source as UTM_Source
	, ia.UTM_Medium as UTM_Medium
    , ia.UTM_Campaign as UTM_Campaign
	, ia.UTM_Term as UTM_Term
	, ia.UTM_Stage as UTM_Stage
	, ia.pipeline as pipeline
	, ia.status as status
	, ia.format as format
	, ia.city as city
	, ia.reason as reason
	, ia.reason_client as reason_client
	, ia.href as href
	, ia.gross as gross
	, ia.finance_source as finance_source
	, ia.client_id as client_id
	, ia.reason_date as reason_date
	, ia.form as form
	, ia.form_class as form_class
	, ia.UTM_Source1 as UTM_Source1
	, ia.UTM_Placement as UTM_Placement
	, ia.client_id_int as client_id_int
	, ia.tags as tags
	, ia.date_close as date_close
	, ia.UTM_retarget as UTM_retarget
	, ia.UTM_phrase as UTM_phrase
	, ia.UTM_field as UTM_field
	, ia.UTM_referrer as UTM_referrer
	, ia.type_payment as type_payment
	, ia.ga_utm as ga_utm
	, ia.roistat as roistat
	, ia.country as country
	, ia.sphere as sphere
	, ia.ad_name as ad_name
	, ia.referrer as referrer
	, ia.openstat_service as openstat_service
	, ia.openstat_campaign as openstat_campaign
	, ia.openstat_ad as openstat_ad
	, ia.openstat_source as openstat_source
	, ia.`from` as `from`
	, ia.gclientid as gclientid
	, ia.gclid as gclid
	, ia.yclid as yclid
	, ia.fbclid as fbclid
	, ia.company as company

	, coalesce(nullif(iv.traffic_source, ''), 'undefined') as traffic_source
	, coalesce(nullif(iv.source, ''), 'undefined') as source
	, coalesce(nullif(iv.medium, ''), 'undefined') as medium
	, coalesce(nullif(iv.campaign, ''), 'undefined') as campaign
	, coalesce(nullif(iv.content, ''), 'undefined') as content

FROM {{ ref('stg_amocrm_united') }} AS ia
	LEFT JOIN intermediate_visits AS iv ON iv.client_id = ia.client_id_int
		and iv.dt = cast(ia.date_create as Date)
		and iv.rn = cast(1 as UInt64)

WHERE 1 = 1
	AND pipeline IN ('manual mode', 'auto mode', 'брак')
--	AND pipeline NOT IN ('брак')
--	AND "type" IN ('Первая')
	AND responsible_user NOT IN ('RAMMBOT', 'unknown')	
	AND "source" NOT IN ('voice')
--	AND form_class NOT IN ('partnerka')
--	AND date_create >= '2021-11-01'