
with parsed as (

SELECT

      `ym:s:visitID` as id
    , visits.company as company
    , `ym:s:date` as dt	
    , `ym:s:dateTimeUTC` as ts
    , `ym:s:clientID` as client_id	

    , case
        -- OVERRIDE
        when `ym:s:UTMSource` IN ('youtube') then 'social'
        else `ym:s:lastTrafficSource`
      end as traffic_source
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
        -- OVERRIDE
        when `ym:s:UTMSource` IN ('getintent') then 'getintent'
        when `ym:s:UTMSource` IN ('youtube') then 'youtube'
        when `ym:s:UTMSource` IN ('yandex.zen', 'yandex.promopages') then 'yandex.zen'
        -- ad
        when traffic_source in ('ad') then coalesce(source_mapping.master, nullif(`ym:s:UTMSource`, ''), 'undefined')
        -- direct
        when traffic_source in ('direct') then 'direct'
        -- internal
        when traffic_source in ('internal') then coalesce(nullif(`ym:s:lastReferalSource`, ''), 'undefined')
        -- organic
        when traffic_source in ('organic') then `ym:s:lastSearchEngine`
        -- referral
        when traffic_source in ('referral') then coalesce(nullif(`ym:s:lastReferalSource`, ''), 'undefined')
        -- email
        when traffic_source in ('email') then coalesce(nullif(`ym:s:lastReferalSource`, ''), 'undefined')
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

    , coalesce(campaign_mapping.campaign_name, nullif(`ym:s:UTMCampaign`, ''), 'undefined') as campaign_raw

    , coalesce(nullif(`ym:s:UTMContent`, ''), 'undefined') as content
    , CASE 
            WHEN ilike(`ym:s:UTMContent`, '%cid%') THEN toUInt64OrNull(splitByChar('|', `ym:s:UTMContent`)[2])
            ELSE toUInt64OrNull(content)
        END AS campaign_id

    , `ym:s:UTMTerm` as keyword
    , cutQueryString(`ym:s:startURL`) as landing_page    
    , CASE `ym:s:deviceCategory`
        WHEN 1 THEN 'DESKTOP'
        WHEN 2 THEN 'MOBILE'
        WHEN 3 THEN 'TABLET'
        WHEN 4 THEN 'TV'
        ELSE 'undefined'
      END AS device
    , `ym:s:regionCity` as region

FROM {{ ref('stg_visits_united') }} as visits
    LEFT ANY JOIN {{ source('gsheet', 'source_mapping') }} as source_mapping on source_mapping.raw = visits.`ym:s:UTMSource`
    LEFT ANY JOIN {{ source('gsheet', 'campaign_mapping') }} as campaign_mapping on campaign_mapping.company = visits.company
        and campaign_mapping.`UTMCampaign` = visits.`ym:s:UTMCampaign`

)

SELECT

      parsed.id as id
    , parsed.company as company
    , parsed.dt as dt
    , parsed.ts as ts
    , parsed.client_id as client_id

    , parsed.traffic_source as traffic_source
    , parsed.traffic_source_importance as traffic_source_importance

    , coalesce(parsed.source, 'undefined') as source
    , parsed.medium as medium  
    , coalesce(
        nullif(cid.campaign_name, ''),
        nullif(gid.campaign_name, ''),
        nullif(aid.campaign_name, ''),
        parsed.campaign_raw) as campaign

    , parsed.content as content

    , keyword
    , landing_page    
    , device
    , region    

    -- DEBUG
    , parsed.campaign_raw as campaign_raw
    , parsed.campaign_id as campaign_id

    , cid.campaign_id as cid_campaign_id
    , gid.adgroup_id as gid_adgroup_id
    , aid.ad_id as aid_ad_id

    , cid.campaign_name as cid_campaign_name
    , gid.campaign_name as gid_campaign_name
    , aid.campaign_name as aid_campaign_name

from parsed
  left any join {{ ref('intermediate_costs_mapping') }} as cid on cid.campaign_id = parsed.campaign_id
  left any join {{ ref('intermediate_costs_mapping') }} as gid on gid.adgroup_id = parsed.campaign_id
  left any join {{ ref('intermediate_costs_mapping') }} as aid on aid.ad_id = parsed.campaign_id

settings join_use_nulls = 0  