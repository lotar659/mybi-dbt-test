-- DROP TABLE dbt.margin_rate ;
CREATE TABLE dbt.margin_rate (
	  `date_month` Date
	, `company` String
	, `margin_rate` Float32
)
ENGINE=URL('http://wh.zmtech.ru/api/deco_info?separator=,', CSVWithNames)
;

SELECT * FROM dbt.margin_rate LIMIT 200 ;

SELECT * FROM dbt.margin_rate LIMIT 200 
SETTINGS input_format_allow_errors_num = 3
, format_csv_allow_double_quotes = 1
, format_csv_allow_single_quotes = 0
;


-- DROP TABLE dbt.source_mapping ;
CREATE TABLE dbt.source_mapping (
	  `raw` String
	, `master` String -- Decimal(32,4)
)
ENGINE=URL('https://docs.google.com/spreadsheets/d/1dQLDrUuB_MfBtXX8LxqbgzZxnigdPkBXw-s6GnnQwBc/gviz/tq?tqx=out:csv&sheet=source_mapping', CSVWithNames)
;

select * from dbt.source_mapping limit 200 ;


-- DROP TABLE dbt.campaign_mapping ;
CREATE TABLE dbt.campaign_mapping (
	  company String
	, `source` String
	, UTMCampaign String
	, campaign_name String
	, is_enabled UInt8
)
ENGINE=URL('https://docs.google.com/spreadsheets/d/1dQLDrUuB_MfBtXX8LxqbgzZxnigdPkBXw-s6GnnQwBc/gviz/tq?tqx=out:csv&sheet=campaign_mapping', CSVWithNames)
;

select * from dbt.campaign_mapping limit 200 ;


-- DROP TABLE dbt.organic_costs ;
CREATE TABLE dbt.organic_costs (
	  `date_month` Date
	, `company` String
	, `cost` Float64
)
ENGINE=URL('https://docs.google.com/spreadsheets/d/1dQLDrUuB_MfBtXX8LxqbgzZxnigdPkBXw-s6GnnQwBc/gviz/tq?tqx=out:csv&sheet=organic', CSVWithNames)
;

select * from dbt.organic_costs limit 200 ;


-- DROP TABLE dbt.zen ;
CREATE TABLE dbt.zen (
	  `date_month` Date
	, `company` String
	, `cost` Float64
)
ENGINE=URL('https://docs.google.com/spreadsheets/d/1dQLDrUuB_MfBtXX8LxqbgzZxnigdPkBXw-s6GnnQwBc/gviz/tq?tqx=out:csv&sheet=zen', CSVWithNames)
;
-- DROP TABLE dbt.programmatik ;
CREATE TABLE dbt.programmatik (
	  `date_month` Date
	, `company` String
	, `cost` Float64
)
ENGINE=URL('https://docs.google.com/spreadsheets/d/1dQLDrUuB_MfBtXX8LxqbgzZxnigdPkBXw-s6GnnQwBc/gviz/tq?tqx=out:csv&sheet=programmatik', CSVWithNames)
;
-- DROP TABLE dbt.tg_bot ;
CREATE TABLE dbt.tg_bot (
	  `date_month` Date
	, `company` String
	, `cost` Float64
)
ENGINE=URL('https://docs.google.com/spreadsheets/d/1dQLDrUuB_MfBtXX8LxqbgzZxnigdPkBXw-s6GnnQwBc/gviz/tq?tqx=out:csv&sheet=tg_bot', CSVWithNames)
;
-- DROP TABLE dbt.web_webinar ;
CREATE TABLE dbt.web_webinar (
	  `date_month` Date
	, `company` String
	, `cost` Float64
)
ENGINE=URL('https://docs.google.com/spreadsheets/d/1dQLDrUuB_MfBtXX8LxqbgzZxnigdPkBXw-s6GnnQwBc/gviz/tq?tqx=out:csv&sheet=web_webinar', CSVWithNames)
;
-- DROP TABLE dbt.event ;
CREATE TABLE dbt.event (
	  `date_month` Date
	, `company` String
	, `cost` Float64
)
ENGINE=URL('https://docs.google.com/spreadsheets/d/1dQLDrUuB_MfBtXX8LxqbgzZxnigdPkBXw-s6GnnQwBc/gviz/tq?tqx=out:csv&sheet=event', CSVWithNames)
;
