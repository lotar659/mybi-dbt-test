SELECT DISTINCT 

	  af.account_id as account_id
	, 'yandex' as adv_engine	  
	  
	, ac.campaign_id as campaign_id
	, ac.name as campaign_name

	, ag.adgroup_id as adgroup_id
	, ag.name as adgroup_name

	, aa.ad_id as ad_id
	, aa.name as ad_name


FROM {{ ref('stg_direct_ads_facts') }} as af
    LEFT JOIN {{ ref('stg_direct_campaigns') }} as ac on ac.id = af.campaigns_id
    LEFT JOIN {{ ref('stg_direct_adgroups') }} as ag on ag.id = af.adgroups_id
    LEFT JOIN {{ ref('stg_direct_ads') }} as aa on aa.id = af.ads_id

settings join_use_nulls = 0