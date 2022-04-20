-----------------------------
--  1. INTERMEDIATE_VISITS --
-----------------------------

select

      id

    , company
    , 'visits' as row_source      
    , dt as create_date

	, traffic_source
	, `source`
	, medium
	, campaign
	, content

    , keyword
    , landing_page    
    , device
    , region


-------------------------
--  intermediate_costs --
-------------------------

	--, account_id
	--, caption
	--, null as campaign_id

	, null as impressions
	, null as clicks
	, null as cost

--------------------------
--  intermediate_visits --
--------------------------

	, client_id

--------------------------
--  intermediate_amocrm --
--------------------------

	, null as lead_id
	, null as date_payment
	, null as reason_date
	, null as date_close

	, null as responsible_user
	, null as status
	, null as pipeline
    , null as type    
	
    , null as price

FROM {{ ref('intermediate_visits') }}

----------------------------
--  2. INTERMEDIATE_COSTS --
----------------------------

union all

select

      id
      
    , company
    , 'costs' as row_source
    , dt as create_date
	
	, traffic_source
    , `source`
	, medium
	, campaign
	, content

    , keyword
    , landing_page    
    , device
    , region

-------------------------
--  intermediate_costs --
-------------------------

	--, account_id
	--, caption
	--, campaign_id

	, impressions
	, clicks
	, cost

--------------------------
--  intermediate_visits --
--------------------------

	, null as client_id
--	, lastTrafficSource    

--------------------------
--  intermediate_amocrm --
--------------------------

	, null as lead_id
	, null as date_payment
	, null as reason_date
	, null as date_close

	, null as responsible_user
	, null as status
	, null as pipeline
    , null as type
	
    , null as price

from {{ ref('intermediate_costs') }}
    left any join {{ ref('intermediate_regions_mapping') }} using (region_id)

-----------------------------
--  3. INTERMEDIATE_AMOCRM --
-----------------------------

union all

select

      
      halfMD5(CONCAT(CAST(id, 'String'), company, 'amocrm')) as id
      
    , company
    , 'amocrm' as row_source      
	, date_create as create_date

	, traffic_source
	, `source`
	, medium
	, campaign
	, content

    , keyword
    , landing_page    
    , device
    , region    
    
-------------------------
--  intermediate_costs --
-------------------------

	--, account_id
	--, caption
	--, null as campaign_id

	, null as impressions
	, null as clicks
	, null as cost

--------------------------
--  intermediate_visits --
--------------------------

	, client_id_int as client_id

--------------------------
--  intermediate_amocrm --
--------------------------

	, id as lead_id
	, date_payment
	, reason_date
	, date_close

	, responsible_user
	, status
	, pipeline
    , type
	
    , price

from {{ ref('intermediate_amocrm') }}

----------------------------------------------
--  4. INTERMEDIATE_AMOCRM – REJECTED LEADS --
----------------------------------------------

union all

select

      halfMD5(CONCAT(CAST(id, 'String'), company, 'amocrm_rejected')) as id
      
    , company
    , 'amocrm_rejected' as row_source      
	, reason_date as create_date

	, traffic_source
	, `source`
	, medium
	, campaign
	, content

    , keyword
    , landing_page    
    , device
    , region    
    
-------------------------
--  intermediate_costs --
-------------------------

	--, account_id
	--, caption
	--, null as campaign_id

	, null as impressions
	, null as clicks
	, null as cost

--------------------------
--  intermediate_visits --
--------------------------

	, client_id_int as client_id

--------------------------
--  intermediate_amocrm --
--------------------------

	, id as lead_id
	, date_payment
	, reason_date
	, date_close

	, responsible_user
	, status
	, pipeline
    , type    
	
    , price

from {{ ref('intermediate_amocrm') }}

where `pipeline` in ('брак') 

-------------------------------------------
--  5. INTERMEDIATE_AMOCRM – FIRST DEALS --
-------------------------------------------

union all

select

      halfMD5(CONCAT(CAST(id, 'String'), company, 'amocrm_first_deals')) as id
      
    , company
    , 'amocrm_first_deals' as row_source      
	, date_payment as create_date

	, traffic_source
	, `source`
	, medium
	, campaign
	, content

    , keyword
    , landing_page    
    , device
    , region    
    
-------------------------
--  intermediate_costs --
-------------------------

	--, account_id
	--, caption
	--, null as campaign_id

	, null as impressions
	, null as clicks
	, null as cost

--------------------------
--  intermediate_visits --
--------------------------

	, client_id_int as client_id

--------------------------
--  intermediate_amocrm --
--------------------------

	, id as lead_id
	, date_payment
	, reason_date
	, date_close

	, responsible_user
	, status
	, pipeline
    , type    
	
    , price

from {{ ref('intermediate_amocrm') }}

where type in ('Первая')
