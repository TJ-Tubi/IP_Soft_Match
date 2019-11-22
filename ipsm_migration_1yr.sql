/*
New viewers who retain within 7 days
*/

with new_viewers as (
  Select DISTINCT u.deviceid,
                  view_ts as first_session_valid,
                  video_session_start_ts
  FROM derived.video_sessions_v2 r JOIN derived.user_signon u
  ON r.deviceid = u.deviceid AND u.filter_tag = 0
  WHERE view_ts >= dateadd('month', -12, date_trunc('month', current_date))
  AND u.view_ts IS NOT NULL
  and r.app in ('tubitv-iphone', 'tubitv-ipad', 'tubitv-android', 'tubitv-firetablet', 'tubitv-android-samsung')
  and cvt > 0
  and u.signup_countries='US'
),

rolling_window AS

  (
    SELECT *,
           CASE
             WHEN video_session_start_ts between (DATEADD('hour', '24', first_session_valid)) and (DATEADD('hour', '672', first_session_valid))
               THEN 1
             ELSE 0 END AS session_between_1_and_28_days
    FROM new_viewers
  ),

maximized AS (
  SELECT deviceid,
         first_session_valid,
         MAX(session_between_1_and_28_days) AS retained
  FROM rolling_window
  GROUP BY 1, 2
)
,
scan_mobile_ips as (
  select distinct deviceid as mobile_device_id,
                  first_session_valid,
                  retained,
                  case when client_ip ilike '%.%' then client_ip
              when client_ipv6 is not NULL and len(client_ipv6)>20 then client_ipv6
              else NULL
         end as ip
  from maximized m left join server_impressions si
  on m.deviceid=si.device_id
)
/*
Returning devices, with view_ts, non-zero viewtime
*/
, eligible_ott_devices as (
  select u.deviceid as ott_device_id, view_ts
  from derived.user_signon u
  join derived.video_sessions_v2 si on u.deviceid=si.deviceid and filter_tag=0
  where view_ts>= dateadd('month', -12, date_trunc('month', current_date))
  and video_session_start_ts>dateadd('hours', 168, view_ts)
  and u.signup_countries='US'
  and u.app in ('tubitv-tvos',
        'tubitv-xbox360',
        'tubitv-xboxone',
        'tubitv-roku',
        'tubitv-for-samsung',
        'tubitv-samsung',
        'tubitv-amazon',
        'tubitv-sony',
        'tubitv-ps4',
        'tubitv-ps3',
        'tubitv-opera',
        'tubitv-tivo',
        'tubitv-androidtv',
        'tubitv-comcast',
        'tubitv-cox',
        'tubitv-vizio')
  group by 1,2
  having sum(cvt)>10
)
, scan_ott_ips as (
  select distinct ott_device_id, view_ts,
         case when client_ip ilike '%.%' then client_ip
              when client_ipv6 is not NULL and len(client_ipv6)>20 then client_ipv6
              else NULL
         end as ip
  from eligible_ott_devices u
  join server_impressions si on u.ott_device_id=si.device_id
)
, mobile_ott_pairs as (
  select distinct o.ott_device_id,
                  m.mobile_device_id,
                  m.first_session_valid,
                  m.retained
                  --identify latest mobile device only
                  ,row_number() over (partition by o.ott_device_id order by m.first_session_valid desc) as mobile_order
  from scan_mobile_ips m left join scan_ott_ips o
    on m.first_session_valid <= o.view_ts
    and m.ip=o.ip
)
, res as (
  SELECT DATE_TRUNC('Month', first_session_valid) as month_,
         COUNT(DISTINCT CASE
                          WHEN retained = 1 and ott_device_id is not NULL
                            and mobile_order = 1
                            then mobile_device_id
                          ELSE NULL END)::float   as retained_paired,
         COUNT(DISTINCT mobile_device_id)::float  as total
  FROM mobile_ott_pairs
  GROUP BY 1
)
select * from res
/*
Express as ratio of monthly viewers; take the ratio, multiply by 0.329 and multiply it by OTT LTV of that month
*/

/*
Sanity check
*/
-- select ott_device_id, count(distinct mobile_device_id) as num_mobile
-- from mobile_ott_pairs
-- group by 1
-- order by num_mobile desc

