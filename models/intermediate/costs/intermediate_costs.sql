
{% set relations_list = [
      ref('intermediate_ads')
    , ref('intermediate_direct')
    , ref('intermediate_facebook')
    , ref('gsheet_organic')
    , ref('gsheet_zen')
    , ref('gsheet_programmatik')
    , ref('gsheet_tg_bot')
    , ref('gsheet_web_webinar')
    , ref('gsheet_event')
] %}

{{ dbt_utils.union_relations(
    relations=relations_list
    ,exclude=['']
) }}
