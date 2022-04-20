SELECT DISTINCT

      `ym:s:regionCityID` as region_id
    , `ym:s:regionCity` as region

FROM {{ ref('stg_visits_united') }}
