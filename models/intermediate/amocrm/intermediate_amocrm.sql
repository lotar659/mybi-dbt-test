select

      id
	, responsible_user
    , company
    , status
    , pipeline
    
    , date_create as dt
    , date_payment
    , reason_date
    , date_close

	, case
        when utm_source in ('ad') then 'cpc'
        else utm_medium
      end as medium

	, case
        when utm_medium in ('cpc')
            and utm_source in ('yandex.context', 'yandex.search', 'yandex', 'yandex.{source_type}') then 'yandex'
        when utm_medium in ('cpc')
            and utm_source in ('google', 'google.search', 'google.context', 'google.{source_type}', 'youtube') then 'google'
        when utm_medium in ('cpc')
            and utm_source in ('Instagram_Feed', 'Facebook_Right_Column', 'Facebook_Mobile_Feed', 'Instagram_Stories', 'Instagram_Explore', 'Facebook_Desktop_Feed', 'Facebook_Stories', 'ig') then 'facebook'
        when utm_source in ('ad') then 'undefined'    
        else utm_source
      end as source

    , utm_campaign as campaign

    , client_id_int
    , price

from {{ ref('intermediate_amocrm_inferred') }}

WHERE 1 = 1
	AND pipeline IN ('manual mode', 'auto mode', 'брак')
--	AND pipeline NOT IN ('брак')
--	AND "type" IN ('Первая')
	AND responsible_user NOT IN ('RAMMBOT', 'unknown')	
	AND "source" NOT IN ('voice')
	AND form_class NOT IN ('partnerka')
--	AND date_create >= '2021-11-01'