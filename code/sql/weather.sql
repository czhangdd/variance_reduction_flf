drop table if exists flf_weather_supply_demand;
create table flf_weather_supply_demand as(
with daypart_mapping as (
  SELECT 
    case when day_part = 'late_night' then 'latenight' 
      else DAY_PART
    END as daypart,
    MIN(LOCAL_HOUR) as min_hour,
    MAX(LOCAL_HOUR) as max_hour
  FROM PRODDB.STATIC.LOOKUP_DAY_PART_MAPPING 
  GROUP BY 1
),

flf_targets as (
SELECT 
  FLF.STARTING_POINT_ID, 
  FLF.STARTING_POINT_NAME, 
  FLF.TIME_OF_DAY as daypart, 
  dp.min_hour,
  dp.max_hour,
  FLF.TARGET_IDEAL_FLF, 
  FLF.MIN_TARGET_FLF_RANGE, 
  FLF.MAX_TARGET_FLF_RANGE, 
  FLF.TARGET_CREATED_AT, 
  LEAD(FLF.TARGET_CREATED_AT, 1) OVER (PARTITION BY FLF.STARTING_POINT_ID, FLF.TIME_OF_DAY ORDER BY FLF.TARGET_CREATED_AT) as next, 
  IFNULL(next, '2022-01-01') AS NEXT_TARGET_CREATED_DATE
FROM STATIC.LOOKUP_TARGET_FLF_BY_REGION flf
LEFT JOIN daypart_mapping dp 
  on flf.TIME_OF_DAY = dp.DAYPART
),
flf_raw as (
SELECT
  dd.created_at,
  dd.DELIVERY_ID,
  dd.active_date,
  dd.STORE_STARTING_POINT_ID,
  dd.SUBMARKET_ID,
  dd.flf,
  fces.num_delivered as num_delivered,
  fces.num_opened as num_opened,
  sm.LAUNCH_DATE as submarket_launch_date,
  convert_timezone('UTC', dd.TIMEZONE, dd.CREATED_AT) as created_at_local,
  TO_DATE(created_at_local) as created_at_local_date,
  hour(created_at_local) * 2 + floor(minute(created_at_local)/30.0) as window_id,
  datediff('second', dd.CREATED_AT, dd.ACTUAL_DELIVERY_TIME)/60.0 as asap,
  dd.DISTINCT_ACTIVE_DURATION/60.0 as dat,
  datediff('second', dd.DASHER_CONFIRMED_TIME, dd.DASHER_AT_STORE_TIME)/60.0 as d2r,
  TRUNC(created_at, 'HOUR') as created_at_hour, 
  case when datediff('second', dd.QUOTED_DELIVERY_TIME, dd.ACTUAL_DELIVERY_TIME)/60 > 20 then 1 else 0 END as lateness_20_min,
  flf.daypart,
  case when dd.flf - flf.MAX_TARGET_FLF_RANGE > 0 then 1 else 0 END as is_flf_above_max,
  case when dd.flf - flf.TARGET_IDEAL_FLF > 0 then 1 else 0 END as is_flf_above_ideal
FROM PRODDB.PUBLIC.DIMENSION_DELIVERIES dd 
LEFT JOIN flf_targets flf
  on dd.STORE_STARTING_POINT_ID = flf.STARTING_POINT_ID
  AND hour(convert_timezone('UTC', dd.TIMEZONE, dd.created_at)) between flf.min_hour and flf.max_hour
  AND convert_timezone('UTC', dd.TIMEZONE, dd.created_at) between flf.TARGET_CREATED_AT and flf.NEXT_TARGET_CREATED_DATE
LEFT JOIN PRODDB.PUBLIC.MAINDB_SUBMARKET sm 
  ON dd.SUBMARKET_ID = sm.ID
LEFT JOIN public.fact_cx_email_summary fces
  ON dd.SUBMARKET_ID = fces.SUBMARKET_ID
  AND convert_timezone('UTC',dd.timezone,
 dateadd('minute',cast(floor(date_part('minute',dd.created_at) / 30) * 30 as int), date_trunc('hour',dd.created_at))
 ) = fces.half_hour_local
WHERE dd.created_at between '2020-03-16' and '2020-04-27'
  AND dd.IS_FILTERED_CORE = true 
  AND dd.IS_ASAP = true 
  AND dd.IS_CONSUMER_PICKUP = false 
  AND fulfillment_type != 'merchant_fleet'
)
,

flf_weather as(
select
  flf_raw.*,
  weather.HH_TEMPERATURE,
  weather.HH_APPARENT_TEMPERATURE,
  weather.HH_PRESSURE,
  weather.HH_HUMIDITY,
  weather.HH_VISIBILITY,
  weather.HH_WIND_SPEED,
  weather.HH_CLOUD_COVER,
  weather.HH_DEWPOINT,
  weather.HH_HOURLY_WEATHER_SUMMARY,
  weather.HH_PRECIP_INTENSITY,
  weather.HH_PRECIP_PROBABILITY,
  weather.HH_ICON,
  weather.HH_PRECIP_ACCUMULATION,
  weather.HH_PRECIP_TYPE
  from flf_raw
left join fact_weather_hour weather
  on weather.hour_utc = flf_raw.created_at_hour
  and weather.STARTING_POINT_ID = flf_raw.STORE_STARTING_POINT_ID
)
,

supply_demand_hist as(
select
      distinct active_date
      , sp_id
      , window_id
      , last_value(incoming_deliveries) over (PARTITION BY active_date, sp_id, window_id ORDER BY process_timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) pred_demand
  from
  public.fact_supply_targets_historical
  where 
  1
  and metadata ilike '%6day%'
  and active_date >= '2020-03-16'::date - interval '8 days'
  and active_date <= '2020-04-27'
//  order by 1, 2, 3, 4
)
,

flf_weather_supply_demand_hist as(
select 
  flf_weather.*,
  PRED_DEMAND
from flf_weather
join supply_demand_hist sdh
  on to_date(flf_weather.created_at) = sdh.active_date
  and flf_weather.window_id = sdh.window_id
  and flf_weather.STORE_STARTING_POINT_ID = sdh.sp_id
)

select * from flf_weather_supply_demand_hist
  )