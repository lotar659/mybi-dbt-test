
with parsed as (

SELECT

	  company
	, `ym:s:date` as dt	
  , `ym:s:dateTimeUTC` as ts
	, `ym:s:clientID` as client_id	
	
  , `ym:s:lastTrafficSource` as traffic_source
  , case
      -- ad
      when traffic_source in ('ad') then 1
      -- direct
      when traffic_source in ('direct') then 2
      -- internal
      when traffic_source in ('internal') then 3
      -- organic
      when traffic_source in ('organic') then 4
      -- referral
      when traffic_source in ('referral') then 5
      -- email
      when traffic_source in ('email') then 6
      -- social
      when traffic_source in ('social') then 7
      -- saved
      when traffic_source in ('saved') then 8
      -- undefined
      when traffic_source in ('undefined') then 9
      -- messenger
      when traffic_source in ('messenger') then 10
      -- recommend
      when traffic_source in ('recommend') then 11
      else 12
    end as traffic_source_importance

  , case
      -- ad
      when traffic_source in ('ad') then coalesce(nullif(sm.source_master, ''), `ym:s:UTMSource`)
      -- direct
      when traffic_source in ('direct') then 'direct'
      -- internal
      when traffic_source in ('internal') then `ym:s:lastReferalSource`
      -- organic
      when traffic_source in ('organic') then `ym:s:lastSearchEngine`
      -- referral
      when traffic_source in ('referral') then `ym:s:lastReferalSource`
      -- email
      when traffic_source in ('email') then `ym:s:lastReferalSource`
      -- social
      when traffic_source in ('social') then `ym:s:lastSocialNetwork`
      -- saved
      when traffic_source in ('saved') then 'saved'
      -- undefined
      when traffic_source in ('undefined') then 'undefined'
      -- messenger
      when traffic_source in ('messenger') and ilike(`ym:s:referer`, '%telegram%') then 'telegram'
      when traffic_source in ('messenger') and ilike(`ym:s:referer`, '%whatsapp%') then 'whatsapp'
      when traffic_source in ('messenger') and ilike(`ym:s:referer`, '%skype%') then 'skype'
      when traffic_source in ('messenger') then `ym:s:referer`
      -- recommend
      when traffic_source in ('recommend') then `ym:s:referer`
      else `ym:s:referer`
    end as source
	
  , coalesce(nullif(`ym:s:UTMMedium`, ''), 'undefined') as medium

	, case `ym:s:UTMCampaign`
      when 'PM_WhatsApp' then 'Performance Max | WhatsApp | РФ'
      else `ym:s:UTMCampaign`
    end as campaign_raw

  , `ym:s:UTMContent` as content
	, CASE 
		  WHEN ilike(`ym:s:UTMContent`, '%cid%') THEN toUInt64OrNull(splitByChar('|', `ym:s:UTMContent`)[2])
		  ELSE toUInt64OrNull(content)
	  END AS campaign_id

FROM {{ ref('stg_visits_united') }} as vu
  left any join {{ ref('source_mapping') }} as sm on sm.source_raw = vu.`ym:s:UTMSource`

)

SELECT

	  parsed.company as company
	, parsed.dt as dt
	, parsed.ts as ts
	, parsed.client_id as client_id
	
  , parsed.traffic_source as traffic_source
  , parsed.traffic_source_importance as traffic_source_importance

  , coalesce(nullif(parsed.source, ''), 'undefined') as source

  , coalesce(nullif(parsed.medium, ''), 'undefined') as medium
  
  , coalesce(nullif(cid.campaign_name, ''), 
      nullif(gid.campaign_name, ''), 
      nullif(aid.campaign_name, ''),
      nullif(parsed.campaign_raw, ''),
      'undefined') as campaign

  , parsed.content as content

-- DEBUG
--  , parsed.campaign_raw as campaign_raw
--  , parsed.content as content
--	, parsed.campaign_id as campaign_id
--  , nullif(cid.campaign_name, '') as cid_campaign_name
--  , nullif(gid.campaign_name, '') as gid_campaign_name
--  , nullif(aid.campaign_name, '') as aid_campaign_name

from parsed
  left any join {{ ref('intermediate_mapping') }} as cid on cid.campaign_id = parsed.campaign_id
  left any join {{ ref('intermediate_mapping') }} as gid on gid.adgroup_id = parsed.campaign_id
  left any join {{ ref('intermediate_mapping') }} as aid on aid.ad_id = parsed.campaign_id