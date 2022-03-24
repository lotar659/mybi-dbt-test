SELECT

	  `id`
	, CAST(`adgroup_id` AS UInt64) as `adgroup_id`
	, `name`

FROM {{ source('mybi', 'ads_adgroups') }}
-- FROM postgresql('rc1a-uifqtk602qck1es3.mdb.yandexcloud.net:6432', 'mybi-niipdbo', 'stg_ads_adgroups', 'owner-niipdbo', 'LAOyHLkcL4L5', 'public')
