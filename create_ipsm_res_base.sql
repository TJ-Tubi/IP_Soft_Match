drop table if exists scratch.ipsm_911_res;

create table scratch.ipsm_911_res
  distkey (mobile_device_id)
  as (
  with registered as (
    select ir.ip, ir.ott_device_id, ir.ott_app, u.deviceid as mobile_device_id, mobile_registered, ii.*
    from (select *, 1 as mobile_registered from scratch.ipsm_registered_911) ir
           join braze.inapp_user_performance ii on ir.mobile_user_id = ii.user_id
           join derived.user_signon u on ir.mobile_user_id = u.user_alias
           join scratch.braze_user_lookup b on b.deviceid = u.deviceid
    where campaign_name in (
                            'In-App_Cross_Device_Mapping_09_05_19_Registered_Users',
                            'In-App_Cross_Device_Mapping_09_11_19_Registered_Users')
      and app in ('tubitv-iphone', 'tubitv-ipad', 'tubitv-android', 'tubitv-firetablet', 'tubitv-android-samsung')
      and has_impression = True
      and filter_tag = 0
      and view_ts is not NULL
      and button_id is not NULL
  )
  , unregistered as (
    select i.ip,
           i.ott_device_id,
           ott_app,
           b.deviceid as mobile_device_id,
           0 as mobile_registered,
           ii.*
    from scratch.ipsm_unregistered_911 i
           join scratch.braze_user_lookup b on i.mobile_braze_id = b.braze_id
           join braze.inapp_user_performance ii on ii.braze_id = i.mobile_braze_id
    where campaign_name in ('In-App_Cross_Device_Mapping_09_05_19_Unregistered_Users',
                            'In-App_Cross_Device_Mapping_09_11_19_Unregistered_Users')
      and has_impression = True
    and button_id is not NULL
  )
  , full_tally as (
    select *
    from registered
    union
    select *
    from unregistered
  )
  , full_tally_labeled as (
    select
           case when button_id is NULL then 1 else button_id end as label,
--            button_id as label,
           *
    from full_tally
    where ip ilike '%.%'
  )
  , mobile_latest_activity as (
  select f.ip, f.mobile_device_id, f.ott_device_id, max(s.ts) as max_mobile_ts
  from server_impressions s
  join full_tally_labeled f on s.client_ip=f.ip and s.device_id=f.mobile_device_id
  GROUP BY 1,2,3
)
, ott_latest_activity as (
  select f.ip, f.ott_device_id, f.mobile_device_id, max(s.ts) as max_ott_ts
  from server_impressions s
  join full_tally_labeled f on s.client_ip=f.ip and s.device_id=f.ott_device_id
  GROUP BY 1,2,3
) , activity_time_gap as (
  select m.ip, m.mobile_device_id, m.ott_device_id, datediff('day', max_mobile_ts, max_ott_ts) timegap
  from mobile_latest_activity m
  join ott_latest_activity o on m.ip=o.ip
  and o.mobile_device_id=o.mobile_device_id
  and o.ott_device_id=m.ott_device_id
)
  , mobile_video_sessions_series as (
    select distinct r.ip as ip, mobile_device_id, series_id
    from full_tally_labeled r
    left join derived.video_sessions_v2 v on r.mobile_device_id=v.deviceid
    where video_session_start_ts between '2019-07-01' and '2019-10-01'
    and video_type='episode'
    and series_id is not NULL
    and autoplay_cvt=0
    and non_autoplay_cvt>0
  ),
  ott_video_sessions_series as (
    select distinct r.ip as ip, ott_device_id, series_id
    from full_tally_labeled r
    left join derived.video_sessions_v2 v on r.ott_device_id=v.deviceid
    where video_session_start_ts between '2019-07-01' and '2019-10-01'
    and video_type='episode'
    and series_id is not NULL
    and autoplay_cvt=0
    and non_autoplay_cvt>0
  )
  , same_series_bin as (
  select i.ip,
         i.mobile_device_id,
         i.ott_device_id,
         count(distinct case when m.series_id=o.series_id then m.series_id else NULL end)
           as num_same_series,
        (count(distinct m.series_id)+count(distinct o.series_id))-num_same_series as num_total_series,
         case when num_total_series>0 then num_same_series::float/num_total_series else 0 end as same_series_ratio
  from full_tally_labeled i
         left join mobile_video_sessions_series m on i.mobile_device_id = m.mobile_device_id and i.ip = m.ip
         left join ott_video_sessions_series o on i.ott_device_id = o.ott_device_id and i.ip = o.ip
  group by i.ip, i.mobile_device_id, i.ott_device_id
)
, mobile_video_sessions_movie as (
  select distinct r.ip as ip, mobile_device_id, video_id
  from full_tally_labeled r
  left join derived.video_sessions_v2 v on r.mobile_device_id=v.deviceid
  where video_session_start_ts between '2019-07-01' and '2019-10-01'
  and video_type='movie'
  and video_id is not NULL
  and autoplay_cvt=0
  and non_autoplay_cvt>0
),
ott_video_sessions_movie as (
  select distinct r.ip as ip, ott_device_id, video_id
  from full_tally_labeled r
  left join derived.video_sessions_v2 v on r.ott_device_id=v.deviceid
  where video_session_start_ts between '2019-07-01' and '2019-10-01'
  and video_type='movie'
  and video_id is not NULL
  and autoplay_cvt=0
  and non_autoplay_cvt>0
)
, same_movie_bin as (
  select i.ip,
         i.mobile_device_id,
         i.ott_device_id,
         count(distinct case when m.video_id=o.video_id then m.video_id else NULL end)
           as num_same_movies,
         (count(distinct m.video_id)+count(distinct o.video_id))-num_same_movies as num_total_movies,
         case when num_total_movies>0 then num_same_movies::float/num_total_movies else 0 end as same_movies_ratio
  from full_tally_labeled i
         left join mobile_video_sessions_movie m on i.mobile_device_id = m.mobile_device_id and i.ip = m.ip
         left join ott_video_sessions_movie o on i.ott_device_id = o.ott_device_id and i.ip = o.ip
  group by i.ip, i.mobile_device_id, i.ott_device_id
),
mobile_video_sessions as (
  select r.ip, deviceid, first_pp_ts , last_pp_ts
  from full_tally_labeled r
  join recent_video_sessions_v2 v on r.mobile_device_id=v.deviceid
  where video_session_start_ts between '2019-07-01' and '2019-10-01'
  and autoplay_cvt=0
  and non_autoplay_cvt>0
),
ott_video_sessions as (
  select r.ip, deviceid, first_pp_ts , last_pp_ts
  from full_tally_labeled r
  join recent_video_sessions_v2 v on r.ott_device_id=v.deviceid
  where video_session_start_ts between '2019-07-01' and '2019-10-01'
  and autoplay_cvt=0
  and non_autoplay_cvt>0
),
session_union as (
  select ip, deviceid, first_pp_ts , last_pp_ts from mobile_video_sessions
  union
  select * from ott_video_sessions
  order by first_pp_ts
)
--Implicit assumption that the same deviceid will not have overlapping video sessions
, session_lead_inclusion as (
  select distinct ip, deviceid as current_deviceid, first_pp_ts  as current_start, last_pp_ts as current_end,
  lead (deviceid, 1 IGNORE NULLS) over (partition by ip order by first_pp_ts ) as next_deviceid,
  lead (first_pp_ts , 1 IGNORE NULLS) over (partition by ip order by first_pp_ts ) as next_start,
  lead (last_pp_ts, 1 IGNORE NULLS) over (partition by ip order by first_pp_ts ) as next_end
  from session_union
)
, video_session_overlap as (
  -- seems this may have been too aggressive; may need a bit more insight into how a video session ends
  select ip,
         sum(case when next_start < dateadd('minutes', -0, current_end) and current_deviceid<>next_deviceid
           then datediff('seconds', next_start, dateadd('minutes', -0, current_end)) else 0 end) as overlap_seconds
  from session_lead_inclusion
  group by ip
)

select f.*,
       s.num_same_series,
       s.same_series_ratio,
       m.num_same_movies,
       m.same_movies_ratio,
       v.overlap_seconds,
       a.timegap
from full_tally_labeled f
join same_series_bin s on f.ip=s.ip and f.mobile_device_id=s.mobile_device_id and f.ott_device_id=s.ott_device_id
join same_movie_bin m on f.ip=m.ip and f.mobile_device_id=m.mobile_device_id and f.ott_device_id=m.ott_device_id
join video_session_overlap v on f.ip=v.ip
join activity_time_gap a on f.ip=a.ip and f.mobile_device_id=a.mobile_device_id and f.ott_device_id=a.ott_device_id
);
GRANT select ON scratch.ipsm_911_res to periscope_readonly; commit;
grant select on table scratch.ipsm_911_res TO GROUP readonlyaccounts; commit;