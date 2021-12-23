SELECT

	  company

	, `ym:s:date` as dt
	
	, `ym:s:clientID` as client_id
	
	, `ym:s:lastTrafficSource` as lastTrafficSource
	-- , `ym:s:lastAdvEngine` as lastAdvEngine

	, `ym:s:UTMMedium` as medium
	, case
        when `ym:s:UTMMedium` in ('cpc')
            and `ym:s:UTMSource` in ('yandex.context', 'yandex.search', 'yandex', 'yandex.{source_type}') then 'yandex'
        when `ym:s:UTMMedium` in ('cpc')
            and `ym:s:UTMSource` in ('google', 'google.search', 'google.context', 'google.{source_type}') then 'google'
        when `ym:s:UTMMedium` in ('cpc')
            and `ym:s:UTMSource` in ('Instagram_Feed', 'Facebook_Right_Column', 'Facebook_Mobile_Feed', 'Instagram_Stories', 'Instagram_Explore', 'Facebook_Desktop_Feed', 'Facebook_Stories', 'ig') then 'facebook'
        else `ym:s:lastTrafficSource`
      end as source
	, `ym:s:UTMCampaign` as campaign

    
FROM {{ ref('stg_visits_united') }}