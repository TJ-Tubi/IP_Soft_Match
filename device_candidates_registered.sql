/*
[x] Active on both devices recently (let’s say 1 month)
[x] Returning Viewers on both devices
[] Non-overlapping non-autoplay viewtime
[x] Minimum VT on both devices (let’s say 15 minutes)
[] Viewers of the same series on both devices
*/
with valid_devices as (
        select case when (v.app IN ('tubitv-tvos',
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
        'tubitv-chromecast',
        'tubitv-androidtv',
        'tubitv-comcast',
        'tubitv-cox')
        and u.deviceid=u.user_alias
          ) --unregistered OTT devices
        or (v.app in ('tubitv-iphone','tubitv-ipad','tubitv-android','tubitv-firetablet','tubitv-android-samsung')
            and u.deviceid<>u.user_alias --registered mobile users
        )

        then u.deviceid end as device_id,
--         max(case when u.deviceid=user_alias then '0' else '1' end) as bool_registered,
        sum(non_autoplay_cvt) as nap_tvt
        from (select * from recent_video_sessions_v2
              where non_autoplay_cvt>0
              and video_session_start_ts >= dateadd('day', -30, CURRENT_DATE)
          ) v
               join (select * from derived.user_signon where filter_tag = 0) u
                    on v.deviceid = u.deviceid
        where v.video_session_start_ts >= DATEADD('day', +7, u.view_ts)
        group by device_id
        having nap_tvt >= 900
       )
,
ip_candidates_registered_mobile as (
  select i.ip, count(distinct platform) as num_platforms
  from (select * from valid_devices
--     where bool_registered='1'
    ) v
  join (select * from scratch.ip_soft_match where platform <> 'Web') i on i.device_id = v.device_id
  group by i.ip
  having num_platforms>1
)
, ip_device_parallel as (
  select i1.ip,
         i2.user_alias as mobile_user_id,
         i3.device_id as ott_device_id,
         substring(i3.app, charindex('-', i3.app)+1, len(i3.app)-charindex('-', i3.app)) as OTT_App
  from ip_candidates_registered_mobile i1
         join (select si.*, u.user_alias from scratch.ip_soft_match si
               join derived.user_signon u on si.device_id=u.deviceid
               join valid_devices vd on vd.device_id=si.device_id
               where platform = 'Mobile'
               and u.user_alias<>u.deviceid and filter_tag=0) i2 on i1.ip = i2.ip
         join (select si.ip, si.device_id, si.app from scratch.ip_soft_match si
         join valid_devices vd on vd.device_id=si.device_id
         where platform = 'OTT') i3 on i1.ip = i3.ip
)
, ott_tvt as (
  select ip, mobile_user_id, ott_device_id, OTT_App, sum(non_autoplay_cvt) as nap_tvt
  from ip_device_parallel i
         join (select non_autoplay_cvt, deviceid from recent_video_sessions_v2 where non_autoplay_cvt > 0) v
              on i.ott_device_id = v.deviceid
  group by ip, mobile_user_id, ott_device_id, OTT_App
)
,
most_active_ott as (
  select distinct ip,
                  mobile_user_id,
                  first_value(ott_device_id) over
                    (partition by ip, mobile_user_id order by nap_tvt desc
                    rows between unbounded preceding and unbounded following)
                    as ott_device_id, --most active ott device
                    first_value(ott_app) over
                    (partition by ip, mobile_user_id order by nap_tvt desc
                    rows between unbounded preceding and unbounded following)
                    as ott_app
  from ott_tvt
)
, ott_video_tvt as (
  select ip, mobile_user_id, ott_device_id, OTT_App, video_id, sum(non_autoplay_cvt) as nap_tvt
  from most_active_ott m
         join (select non_autoplay_cvt, deviceid, video_id from recent_video_sessions_v2 where non_autoplay_cvt > 0) v
              on m.ott_device_id = v.deviceid
  group by ip, mobile_user_id, ott_device_id, OTT_App, video_id
)
,
most_active_ott_video as (
  select distinct ip,
                  mobile_user_id, ott_device_id, OTT_App,
                  first_value(video_id) over
                    (partition by ott_device_id order by nap_tvt desc
                    rows between unbounded preceding and unbounded following)
                    as video_id, --most active ott device
                    first_value(nap_tvt) over
                    (partition by ott_device_id order by nap_tvt desc
                    rows between unbounded preceding and unbounded following)
                    as nap_tvt
  from ott_video_tvt
)
, res as (
  select m.*, c.title
  from most_active_ott_video m
         join (select title, video_id from content_v2) c on m.video_id = c.video_id
)
select * from res
-- select top 100 * from res
-- order by nap_tvt asc