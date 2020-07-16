CREATE TEMP TABLE PUBLIC.{tablename}_DAYPART_MAPPING AS (
  SELECT 
    CASE WHEN day_part = 'late_night' THEN 'latenight' 
      ELSE DAY_PART
    END AS daypart,
    MIN(LOCAL_HOUR) AS min_hour,
    MAX(LOCAL_HOUR) AS max_hour
  FROM PRODDB.STATIC.LOOKUP_DAY_PART_MAPPING 
  GROUP BY 1
);
  
CREATE temp TABLE PUBLIC.{tablename}_FLF_TARGETS AS (
SELECT 
  FLF.STARTING_POINT_ID, 
  FLF.STARTING_POINT_NAME, 
  FLF.TIME_OF_DAY AS daypart, 
  dp.min_hour,
  dp.max_hour,
  FLF.TARGET_IDEAL_FLF, 
  FLF.MIN_TARGET_FLF_RANGE, 
  FLF.MAX_TARGET_FLF_RANGE, 
  FLF.TARGET_CREATED_AT, 
  LEAD(FLF.TARGET_CREATED_AT, 1) OVER (PARTITION BY FLF.STARTING_POINT_ID, FLF.TIME_OF_DAY ORDER BY FLF.TARGET_CREATED_AT) AS NEXT, 
  IFNULL(NEXT, '2022-01-01') AS NEXT_TARGET_CREATED_DATE
FROM STATIC.LOOKUP_TARGET_FLF_BY_REGION flf
LEFT JOIN PUBLIC.{tablename}_DAYPART_MAPPING dp 
  ON flf.TIME_OF_DAY = dp.DAYPART
);
    
CREATE temp TABLE PUBLIC.{tablename}_FLF_RAW AS (
SELECT
  dd.created_at,
  dd.DELIVERY_ID,
  dd.active_date,
  dd.STORE_STARTING_POINT_ID,
  dd.SUBMARKET_ID,
  dd.flf,
  dd.DASHER_PAY_OTHER,
  fces.num_delivered AS num_delivered,
  fces.num_opened AS num_opened,
  sm.LAUNCH_DATE AS submarket_launch_date,
  convert_timezone('UTC', dd.TIMEZONE, dd.CREATED_AT) AS created_at_local,
  TO_DATE(created_at_local) AS created_at_local_date,
  DAYOFWEEK(created_at_local) as day_of_week,
  hour(created_at_local) as hour_of_day,  
  hour(created_at_local) * 2 + floor(minute(created_at_local)/30.0) AS window_id,
  datediff('second', dd.CREATED_AT, dd.ACTUAL_DELIVERY_TIME)/60.0 AS asap,
  dd.DISTINCT_ACTIVE_DURATION/60.0 AS dat,
  datediff('second', dd.DASHER_CONFIRMED_TIME, dd.DASHER_AT_STORE_TIME)/60.0 as d2r,
  CASE WHEN datediff('second', dd.QUOTED_DELIVERY_TIME, dd.ACTUAL_DELIVERY_TIME)/60 > 20 THEN 1 ELSE 0 END AS lateness_20_min,
  flf.daypart,
  CASE WHEN dd.flf - flf.MAX_TARGET_FLF_RANGE > 0 THEN 1 ELSE 0 END AS is_flf_above_max,
  CASE WHEN dd.flf - flf.TARGET_IDEAL_FLF > 0 THEN 1 ELSE 0 END AS is_flf_above_ideal
FROM PRODDB.PUBLIC.DIMENSION_DELIVERIES dd 
LEFT JOIN PUBLIC.{tablename}_FLF_TARGETS flf
  ON dd.STORE_STARTING_POINT_ID = flf.STARTING_POINT_ID
  AND hour(convert_timezone('UTC', dd.TIMEZONE, dd.created_at)) BETWEEN flf.min_hour AND flf.max_hour
  AND convert_timezone('UTC', dd.TIMEZONE, dd.created_at) BETWEEN flf.TARGET_CREATED_AT AND flf.NEXT_TARGET_CREATED_DATE
LEFT JOIN PRODDB.PUBLIC.MAINDB_SUBMARKET sm 
  ON dd.SUBMARKET_ID = sm.ID
LEFT JOIN public.fact_cx_email_summary fces
  ON dd.SUBMARKET_ID = fces.SUBMARKET_ID
  AND convert_timezone('UTC',dd.timezone,
 dateadd('minute',CAST(floor(date_part('minute',dd.created_at) / 30) * 30 AS INT), date_trunc('hour',dd.created_at))
 ) = fces.half_hour_local
  WHERE CAST(DD.CREATED_AT as DATE) >= dateadd('DAY', -7, TO_TIMESTAMP_NTZ(LOCALTIMESTAMP)) 
  AND CAST(DD.CREATED_AT as DATE) < dateadd('DAY', 0, TO_TIMESTAMP_NTZ(LOCALTIMESTAMP))  
  AND dd.IS_FILTERED_CORE = true 
  AND dd.IS_ASAP = true 
  AND dd.IS_CONSUMER_PICKUP = false 
  AND fulfillment_type != 'merchant_fleet'
);

CREATE TEMP TABLE PUBLIC.{tablename}_FLF_RAW_HOURLY_AVG_FLF as(
select STORE_STARTING_POINT_ID
     , date_trunc('HOUR', created_at) as created_at_date_hour
     , avg(flf) as hourly_avg_flf
     , avg(is_flf_above_ideal) as hourly_avg_is_flf_above_ideal
     , avg(is_flf_above_max) as hourly_avg_is_flf_above_max
     , avg(DASHER_PAY_OTHER) as hourly_avg_DASHER_PAY_OTHER
from PUBLIC.{tablename}_FLF_RAW
group by 1, 2
);

CREATE TEMP TABLE PUBLIC.{tablename}_FLF_RAW_HIST as(
select 
  raw.created_at,
  raw.delivery_id,
  raw.day_of_week,
  raw.hour_of_day,
  raw.daypart,
  raw.STORE_STARTING_POINT_ID,
  raw.submarket_id,
  raw.window_id,
  raw.is_flf_above_ideal,
  hist.hourly_avg_flf as pw_hourly_avg_flf,
  hist.hourly_avg_is_flf_above_ideal as pw_hourly_avg_is_flf_above_ideal,
  hist.hourly_avg_is_flf_above_max as pw_hourly_avg_is_flf_above_max,
  hist.hourly_avg_DASHER_PAY_OTHER as pw_hourly_avg_DASHER_PAY_OTHER
from PUBLIC.{tablename}_FLF_RAW raw
left join PUBLIC.{tablename}_FLF_RAW_HOURLY_AVG_FLF hist 
  on
      hist.created_at_date_hour  = date_trunc('HOUR', (dateadd(day, -7, raw.created_at))) --same hour 7 days ago
  and hist.STORE_STARTING_POINT_ID = raw.STORE_STARTING_POINT_ID -- same sp
);


CREATE TEMP TABLE PUBLIC.{tablename}_FLF_WEATHER as(
select
  flf_raw_hist.*,
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
  from PUBLIC.{tablename}_FLF_RAW_HIST flf_raw_hist
left join fact_weather_hour weather
  on weather.hour_utc = TRUNC(flf_raw_hist.created_at, 'HOUR')
  and weather.STARTING_POINT_ID = flf_raw_hist.STORE_STARTING_POINT_ID
);


CREATE TEMP TABLE PUBLIC.{tablename}_ACTUAL_SUPPLY_DEMAND as(
SELECT
      active_date
      , starting_point_id as sp_id
      , half_hour_local_index as window_id
      , max(num_delivs) actual_demand
    FROM
        public.fact_dpred_ml_inputs mli
    WHERE 1
        AND active_date >= TO_TIMESTAMP_NTZ(LOCALTIMESTAMP)::date - interval '8 days'
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3  
);

CREATE TEMP TABLE PUBLIC.{tablename}_SUPPLY_DEMAND as(
   select
       supply_target as pred_demand,
       actual_demand,
       actual_demand - pred_demand as under_predicted_demand,
       t1.active_date,
       t1.window_id,
       t1.sp_id
    from PUBLIC.{tablename}_ACTUAL_SUPPLY_DEMAND t1
    join fact_supply_targets_historical t2
        on t1.active_date = t2.active_date 
       and t1.sp_id = t2.sp_id
       and t1.window_id = t2.window_id
);


CREATE TEMP TABLE PUBLIC.{tablename}_VARIANCE_REDUCTION_FLF as(
select 
    CREATED_AT,
    DELIVERY_ID,
    DAY_OF_WEEK,
    HOUR_OF_DAY,
    DAYPART,
    STORE_STARTING_POINT_ID,
    SUBMARKET_ID,
    flf_weather.WINDOW_ID,
    hh_temperature, 
    hh_apparent_temperature,
    hh_pressure, 
    hh_humidity,
    hh_visibility, 
    hh_wind_speed,
    hh_cloud_cover, 
    hh_dewpoint,
    hh_precip_intensity,
    hh_precip_probability,
    hh_hourly_weather_summary, 
    hh_icon,
    sdh.pred_demand, 
    sdh.actual_demand,
    sdh.under_predicted_demand,
    pw_hourly_avg_flf, 
    pw_hourly_avg_is_flf_above_ideal,
    pw_hourly_avg_is_flf_above_max,
    pw_hourly_avg_dasher_pay_other,
    IS_FLF_ABOVE_IDEAL
from PUBLIC.{tablename}_FLF_WEATHER flf_weather
join PUBLIC.{tablename}_SUPPLY_DEMAND sdh
  on to_date(flf_weather.created_at) = sdh.active_date
  and flf_weather.window_id = sdh.window_id
  and flf_weather.STORE_STARTING_POINT_ID = sdh.sp_id
);

