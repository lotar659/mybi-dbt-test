SELECT DISTINCT 

	  af.account_id as account_id
	, 'facebook' as adv_engine
	  
	, ac.campaign_id as campaign_id
	, ac.name as campaign_name

	, ag.adset_id as adset_id
	, ag.name as adset_name

	, aa.ad_id as ad_id
	, aa.name as ad_name


FROM {{ ref('stg_facebook_ads_facts') }} as af
    LEFT JOIN {{ ref('stg_facebook_campaigns') }} as ac on ac.id = af.campaigns_id
    LEFT JOIN {{ ref('stg_facebook_adsets') }} as ag on ag.id = af.adsets_id
    LEFT JOIN {{ ref('stg_facebook_ads') }} as aa on aa.id = af.ads_id

settings join_use_nulls = 0