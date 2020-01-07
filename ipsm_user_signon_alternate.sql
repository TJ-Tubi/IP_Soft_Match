/*
Algorithm for user-signon based IP dataset

[x] find all user_alias with multiple device_ids
[x] at least two platforms
[x] registered only
[x] active in last year
[x] choose most active device of each platform
*/

drop table if exists scratch.ipsm_user_signon_alternate;

create table scratch.ipsm_user_signon_alternate
  distkey (mobile_device_id)
  as (
with registered_recently_active_tally as (
select us.user_alias, us.deviceid,
       case when us.app IN ('tubitv-tvos',
        'tubitv-xbox360',
        'tubitv-xboxone',
        'tubitv-roku',
        'tubitv-for-samsung',
        'tubitv-samsung',
        'tubitv-amazon',
        'tubitv-sony',
        'tubitv-ps4',
        'tubitv-ps3',
        'tubitv-tivo',
        'tubitv-androidtv',
        'tubitv-comcast',
        'tubitv-cox',
        'tubitv-vizio',
        'tubitv-enseo',
        'tubitv-echoshow')
        then 'OTT'
        when us.app in ('tubitv-iphone','tubitv-ipad','tubitv-android','tubitv-firetablet','tubitv-android-samsung')
        then 'Mobile' else NULL end as platform,
       num_impressions
from (
  select device_id, count(*) as num_impressions
  from server_impressions
--   where ts>=dateadd('week', -1, CURRENT_DATE)
  where ts between '2018-10-01' and '2019-10-08'
  group by device_id
  ) si
join (
  select user_alias, deviceid, app
  from derived.user_signon
  where filter_tag=0
  and deviceid<>user_alias
  and app<>'tubitv-web'
  and signup_countries='US'
  ) us
on si.device_id=us.deviceid
where platform is not NULL
)
, count_platforms as (
  select user_alias, count(distinct platform) as num_platforms
  from registered_recently_active_tally
  group by 1
)
, two_platform_filter as (
  select distinct r.*
  from registered_recently_active_tally r
  join count_platforms c
  on r.user_alias=c.user_alias
  where c.num_platforms=2
)
/*
For each user_alias, rank a device based on impressions and partitioned by platform
*/
, rank_device_activity as(
  select t.user_alias, t.deviceid, t.platform, t.num_impressions,
         row_number() over (partition by t.user_alias, t.platform order by t.num_impressions desc) as activity_rank
  from two_platform_filter t
)
, most_active_devices_with_ip as (
  select distinct r.user_alias, r.deviceid, r.platform, i.ip
  from rank_device_activity r
  join (select * from scratch.ip_soft_match where platform <> 'Web') i
  on r.deviceid=i.device_id
  where activity_rank=1
)
, full_tally_labeled as (
  select m.deviceid || o.deviceid                                     as pairing_id,
         m.deviceid                                                   as mobile_device_id,
         o.deviceid                                                   as ott_device_id,
         m.ip,
         max(case when m.user_alias = o.user_alias then 1 else 0 end) as label
  from (select * from most_active_devices_with_ip where platform = 'OTT') o
         join (select * from most_active_devices_with_ip where platform = 'Mobile') m
              on o.ip = m.ip
  group by 1, 2, 3, 4
)
, mobile_latest_activity as (
  select f.ip,
         f.pairing_id,
         max(s.ts) as max_mobile_ts
  from server_impressions s
  join full_tally_labeled f on
    s.client_ip=f.ip and
    s.device_id=f.mobile_device_id
  GROUP BY 1,2
)
, ott_latest_activity as (
  select f.ip,
         f.pairing_id,
         max(s.ts) as max_ott_ts
  from server_impressions s
  join full_tally_labeled f on
    s.client_ip=f.ip and
    s.device_id=f.ott_device_id
  GROUP BY 1,2
) , activity_time_gap as (
  select m.ip,
         m.pairing_id,
         datediff('day', max_mobile_ts, max_ott_ts) timegap
  from mobile_latest_activity m
  join ott_latest_activity o on m.pairing_id=o.pairing_id and m.ip=o.ip
)
  , mobile_video_sessions_series as (
    select distinct
                    r.ip as ip,
                    pairing_id,
                    mobile_device_id, series_id
    from full_tally_labeled r
    left join derived.video_sessions_v2 v on r.mobile_device_id=v.deviceid
    where video_session_start_ts between '2018-10-01' and '2019-10-01'
    and video_type='episode'
    and series_id is not NULL
    and autoplay_cvt=0
    and non_autoplay_cvt>0
  ),
  ott_video_sessions_series as (
    select distinct
                    r.ip as ip,
                    pairing_id,
                    ott_device_id, series_id
    from full_tally_labeled r
    left join derived.video_sessions_v2 v on r.ott_device_id=v.deviceid
    where video_session_start_ts between '2018-10-01' and '2019-10-01'
    and video_type='episode'
    and series_id is not NULL
    and autoplay_cvt=0
    and non_autoplay_cvt>0
  )
  , same_series_bin as (
  select
         i.ip,
         i.pairing_id,
         i.mobile_device_id,
         i.ott_device_id,
         count(distinct case when m.series_id=o.series_id then m.series_id else NULL end)
           as num_same_series,
        (count(distinct m.series_id)+count(distinct o.series_id))-num_same_series as num_total_series,
         case when num_total_series>0 then num_same_series::float/num_total_series else 0 end as same_series_ratio
  from full_tally_labeled i
         left join mobile_video_sessions_series m on i.mobile_device_id = m.mobile_device_id
                                                       and i.ip = m.ip
         left join ott_video_sessions_series o on i.ott_device_id = o.ott_device_id
                                                    and i.ip = o.ip
  group by
           i.ip,
           i.pairing_id,
           i.mobile_device_id, i.ott_device_id
)
, mobile_video_sessions_movie as (
  select distinct
                  r.ip as ip,
                  pairing_id,
                  mobile_device_id, video_id
  from full_tally_labeled r
  left join derived.video_sessions_v2 v on r.mobile_device_id=v.deviceid
  where video_session_start_ts between '2018-10-01' and '2019-10-01'
  and video_type='movie'
  and video_id is not NULL
  and autoplay_cvt=0
  and non_autoplay_cvt>0
),
ott_video_sessions_movie as (
  select distinct
                  r.ip as ip,
                  pairing_id,
                  ott_device_id, video_id
  from full_tally_labeled r
  left join derived.video_sessions_v2 v on r.ott_device_id=v.deviceid
  where video_session_start_ts between '2018-10-01' and '2019-10-01'
  and video_type='movie'
  and video_id is not NULL
  and autoplay_cvt=0
  and non_autoplay_cvt>0
)
, same_movie_bin as (
  select
         i.ip,
         i.pairing_id,
         i.mobile_device_id,
         i.ott_device_id,
         count(distinct case when m.video_id=o.video_id then m.video_id else NULL end)
           as num_same_movies,
         (count(distinct m.video_id)+count(distinct o.video_id))-num_same_movies as num_total_movies,
         case when num_total_movies>0 then num_same_movies::float/num_total_movies else 0 end as same_movies_ratio
  from full_tally_labeled i
         left join mobile_video_sessions_movie m on i.mobile_device_id = m.mobile_device_id
                                                      and i.ip = m.ip
         left join ott_video_sessions_movie o on i.ott_device_id = o.ott_device_id
                                                   and i.ip = o.ip
  group by
           i.ip,
           i.pairing_id,
           i.mobile_device_id, i.ott_device_id
),
mobile_video_sessions as (
  select
         r.ip,
         pairing_id,
         deviceid,
         first_pp_ts,
         last_pp_ts,
         non_autoplay_cvt,
         autoplay_cvt
  from full_tally_labeled r
  join derived.video_sessions_v2 v on r.mobile_device_id=v.deviceid
  where video_session_start_ts between '2018-10-01' and '2019-10-01'
  and cvt>0
--   and autoplay_cvt=0
--   and non_autoplay_cvt>0
),
ott_video_sessions as (
  select
         r.ip,
         pairing_id,
         deviceid,
         first_pp_ts,
         last_pp_ts,
         non_autoplay_cvt,
         autoplay_cvt
  from full_tally_labeled r
  join derived.video_sessions_v2 v on r.ott_device_id=v.deviceid
  where video_session_start_ts between '2018-10-01' and '2019-10-01'
  and cvt>0
--   and autoplay_cvt=0
--   and non_autoplay_cvt>0
)
,
session_union as (
  select
         ip,
         pairing_id,
         deviceid,
         first_pp_ts,
         last_pp_ts,
         non_autoplay_cvt,
         autoplay_cvt
  from mobile_video_sessions
  union
  select * from ott_video_sessions
  order by first_pp_ts
)
--Implicit assumption that the same deviceid will not have overlapping video sessions
, session_lead_inclusion as (
  select distinct
                  ip,
                  pairing_id,
                  deviceid as current_deviceid,
                  first_pp_ts  as current_start,
                  last_pp_ts as current_end,
                  non_autoplay_cvt,
                  autoplay_cvt,
  lead (deviceid, 1 IGNORE NULLS) over (partition by pairing_id, ip order by first_pp_ts ) as next_deviceid,
  lead (first_pp_ts , 1 IGNORE NULLS) over (partition by pairing_id, ip order by first_pp_ts ) as next_start,
  lead (last_pp_ts, 1 IGNORE NULLS) over (partition by pairing_id, ip order by first_pp_ts ) as next_end
  from session_union
)
, video_session_overlap as (
  -- seems this may have been too aggressive; may need a bit more insight into how a video session ends
  select
         ip,
         pairing_id,
         sum(case when next_start < dateadd('minutes', -0, current_end) and current_deviceid<>next_deviceid
           then datediff('seconds', next_start, dateadd('minutes', -0, current_end)) else 0 end)
           as overlap_seconds,
         sum(non_autoplay_cvt) as total_nap_tvt,
         sum(autoplay_cvt) as total_ap_tvt,
         case when total_nap_tvt>0 then overlap_seconds::float/total_nap_tvt else 0 end as overlap_ratio
  from session_lead_inclusion
  group by pairing_id, ip
)

select f.*,
       s.num_same_series,
       s.same_series_ratio,
       m.num_same_movies,
       m.same_movies_ratio,
       v.overlap_seconds,
       v.total_nap_tvt,
       v.total_ap_tvt,
       v.overlap_ratio,
       a.timegap
from full_tally_labeled f
join same_series_bin s on f.pairing_id=s.pairing_id and f.ip=s.ip
join same_movie_bin m on f.pairing_id=m.pairing_id and f.ip=m.ip
join video_session_overlap v on f.pairing_id=v.pairing_id and f.ip=v.ip
join activity_time_gap a on f.pairing_id=a.pairing_id and f.ip=a.ip

);
GRANT select ON scratch.ipsm_user_signon_alternate to periscope_readonly; commit;
grant select on table scratch.ipsm_user_signon_alternate TO GROUP readonlyaccounts; commit;