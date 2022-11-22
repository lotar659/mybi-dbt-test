SELECT
	id
	, date_create
	, responsible_user
	, price
	, `source`
	, `type`
	, date_payment
	, UTM_Campaign
	, UTM_Content
	, UTM_Medium
	, UTM_Source
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
	, 'smsdar' as company
FROM {{ source('amocrm', 'smsdar') }}
WHERE 1=1
    AND (
        date_create <= '2022-04-30 18:00:00'
            OR `source` != 'referal'
    )
    

UNION ALL 

SELECT
	id
	, date_create
	, responsible_user
	, price
	, `source`
	, `type`
	, date_payment
	, UTM_Campaign
	, UTM_Content
	, UTM_Medium
	, UTM_Source
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
	, 'smspobeda' as company
FROM {{ source('amocrm', 'smspobeda') }}
WHERE 1=1
    AND (
        date_create <= '2021-03-31 18:00:00'
            OR (
                `source` != 'instagram'
                    OR UTM_Campaign != 'maxim'
            )
    )
    AND (
        date_create <= '2022-03-31 18:00:00'
            OR tags NOT LIKE '%(gfeconf)%'
    )
    AND (
        date_create <= '2022-04-30 18:00:00'
            OR `source` != 'referal'
    ) 

UNION ALL

SELECT
	id
	, date_create
	, responsible_user
	, price
	, `source`
	, `type`
	, date_payment
	, UTM_Campaign
	, UTM_Content
	, UTM_Medium
	, UTM_Source
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
	, 'zazumedia' as company
FROM {{ source('amocrm', 'zazumedia') }}
WHERE 1=1
    AND (
        date_create <= '2021-05-31 18:00:00'
            OR (
                `source` != 'instagram'
                    AND `source` != 'facebook'
            )
    )
    AND (
        date_create <= '2022-04-30 18:00:00'
            OR `source` != 'referal'
    )
