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

        , keyword
        , landing_page    
        , device
        , region

		--, traffic_source_importance
		, row_number() over (partition by client_id, dt order by traffic_source_importance asc) as rn	
		, row_number() over (partition by client_id order by traffic_source_importance asc) as rn_global	

	from {{ ref('intermediate_visits') }}
	where client_id not in (0)

	settings allow_experimental_window_functions = 1

)

SELECT

	  amocrm.id as id
	, amocrm.date_create as date_create
	, amocrm.responsible_user as responsible_user
	, amocrm.price as price
	, amocrm.`source` as `source_raw`
	, amocrm.`type` as `type`
	, amocrm.date_payment as date_payment
	, amocrm.UTM_Content as UTM_Content
	, amocrm.UTM_Source as UTM_Source
	, amocrm.UTM_Medium as UTM_Medium
    , amocrm.UTM_Campaign as UTM_Campaign
	, amocrm.UTM_Term as UTM_Term
	, amocrm.UTM_Stage as UTM_Stage
	, amocrm.pipeline as pipeline
	, amocrm.status as status
	, amocrm.format as format
	, amocrm.city as city
	, amocrm.reason as reason
	, amocrm.reason_client as reason_client
	, amocrm.href as href
	, amocrm.gross as gross
	, amocrm.finance_source as finance_source
	, amocrm.client_id as client_id
	, amocrm.reason_date as reason_date
	, amocrm.form as form
	, amocrm.form_class as form_class
	, amocrm.UTM_Source1 as UTM_Source1
	, amocrm.UTM_Placement as UTM_Placement
	, amocrm.client_id_int as client_id_int
	, amocrm.tags as tags
	, amocrm.date_close as date_close
	, amocrm.UTM_retarget as UTM_retarget
	, amocrm.UTM_phrase as UTM_phrase
	, amocrm.UTM_field as UTM_field
	, amocrm.UTM_referrer as UTM_referrer
	, amocrm.type_payment as type_payment
	, amocrm.ga_utm as ga_utm
	, amocrm.roistat as roistat
	, amocrm.country as country
	, amocrm.sphere as sphere
	, amocrm.ad_name as ad_name
	, amocrm.referrer as referrer
	, amocrm.openstat_service as openstat_service
	, amocrm.openstat_campaign as openstat_campaign
	, amocrm.openstat_ad as openstat_ad
	, amocrm.openstat_source as openstat_source
	, amocrm.`from` as `from`
	, amocrm.gclientid as gclientid
	, amocrm.gclid as gclid
	, amocrm.yclid as yclid
	, amocrm.fbclid as fbclid
	, amocrm.company as company

	, coalesce(visits_date_create.traffic_source, visits_date_payment.traffic_source, visits_no_date.traffic_source, 'undefined') as traffic_source

	, case 
		when (amocrm.UTM_Source in ('referal') or amocrm.`source` in ('referal')) then 'referal'
		else coalesce(visits_date_create.source, visits_date_payment.source, visits_no_date.source, 'undefined')
	  end as source

	, case 
		when amocrm.UTM_Source in ('referal') then 'undefined'
		else coalesce(visits_date_create.medium, visits_date_payment.medium, visits_no_date.medium, 'undefined')
	  end as medium

	, case 
		when amocrm.UTM_Source in ('referal') then 'undefined'
		else coalesce(visits_date_create.campaign, visits_date_payment.campaign, visits_no_date.campaign, 'undefined')
	  end as campaign

	, case 
		when amocrm.UTM_Source in ('referal') then amocrm.UTM_Content
		else coalesce(visits_date_create.content, visits_date_payment.content, visits_no_date.content, 'undefined')
	  end as content

    , coalesce(visits_date_create.keyword, visits_date_payment.keyword, visits_no_date.keyword, 'undefined') as keyword
    , cutQueryString(amocrm.href) as landing_page
    , coalesce(visits_date_create.device, visits_date_payment.device, visits_no_date.device, 'undefined') as device
    , coalesce(visits_date_create.region, visits_date_payment.region, visits_no_date.region, 'undefined') as region

    , case
        when amocrm.form in ('Регистрация через webhookBlog') then 1
        when amocrm.UTM_Source in ('blog') then 1
        when amocrm.UTM_Source1 in ('blog') then 1
        when amocrm.`source` in ('blog') then 1
        when amocrm.UTM_Campaign ilike '%blog%' then 1
        else 0
      end as is_blog

FROM {{ ref('stg_amocrm_united') }} AS amocrm
	LEFT JOIN intermediate_visits AS visits_date_create ON visits_date_create.client_id = amocrm.client_id_int
		and visits_date_create.dt = cast(amocrm.date_create as Date)
		and visits_date_create.rn = cast(1 as UInt64)
	LEFT JOIN intermediate_visits AS visits_date_payment ON visits_date_payment.client_id = amocrm.client_id_int
		and visits_date_payment.dt = cast(amocrm.date_payment as Date)
		and visits_date_payment.rn = cast(1 as UInt64)
	LEFT JOIN intermediate_visits AS visits_no_date ON visits_no_date.client_id = amocrm.client_id_int
		and visits_no_date.rn_global = cast(1 as UInt64)        

WHERE 1 = 1
	AND amocrm.pipeline IN ('manual mode', 'auto mode', 'брак')
--	AND pipeline NOT IN ('брак')
--	AND "type" IN ('Первая')
	AND amocrm.responsible_user NOT IN ('RAMMBOT', 'unknown')	
	AND amocrm.source NOT IN ('voice')
--	AND form_class NOT IN ('partnerka')
--	AND date_create >= '2021-11-01'
    AND amocrm.id NOT IN (31322112, 30296008)

settings join_use_nulls = 1