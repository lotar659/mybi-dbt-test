WITH parsed AS (
    
    SELECT

        id
        , `source`
        , medium
        , campaign

        , content
        , splitByChar('|', assumeNotNull(content)) AS content_array
        , indexOf(splitByChar('|', assumeNotNull(content)), 'region_id') AS region_id_index
        , IF(region_id_index = 0, 0, CAST(toFloat32OrZero(content_array[region_id_index+1]) AS UInt32)) as region_id
        
        , keyword
        , traffic_hash
        , `grouping`
        , landing_page

    FROM {{ source('mybi', 'general_traffic') }}

)

    SELECT

        id
        , `source`
        , medium
        , campaign

        , content
        , content_array
        , region_id
        
        , keyword
        , traffic_hash
        , `grouping`
        , landing_page

    FROM parsed
