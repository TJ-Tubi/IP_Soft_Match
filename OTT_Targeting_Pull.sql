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
        'tubitv-cox')
        then 'OTT'
        when us.app in ('tubitv-iphone','tubitv-ipad','tubitv-android','tubitv-firetablet','tubitv-android-samsung')
        then 'Mobile' else NULL end as platform
from (
  select device_id from server_impressions where ts>=dateadd('year', -1, CURRENT_DATE)
  group by device_id
--   having count(*)>10
  ) si
join (
  select user_alias, deviceid, app
  from derived.user_signon where filter_tag=0
  and deviceid<>user_alias
  and app<>'tubitv-web'
  and signup_countries='US'
  ) us
on si.device_id=us.deviceid
where platform is not NULL
)
,
count_platforms as (
  select distinct user_alias, listagg(distinct platform, ',')
    within group(order by deviceid)
    over (partition by user_alias) as platform_str_agg
  from registered_recently_active_tally
)
, registered_mobile_only as (
  select *
  from registered_recently_active_tally r
         join (select user_alias from count_platforms where platform_str_agg = 'Mobile') c
              on r.user_alias = c.user_alias
)
/*
Next step: Join with IPSM for those w/o an OTT on same primary IP

So, look for ip aggregations (using list_agg) where only mobile is on primary IP

Then join back with above for final results

Split into 2 cohorts
*/
, multidevice_ip_str_agg as (
  select distinct ip,
  listagg(distinct platform, ',') within group (order by device_id)
  over (partition by ip) as platform_str_agg
  from scratch.ip_soft_match
  where platform<>'Web'
)

, mobile_only_ips as (
  select *
  from multidevice_ip_str_agg
  where platform_str_agg='Mobile'
)
, ip_mobile_only_devices as (
  select distinct device_id
  from scratch.ip_soft_match i join mobile_only_ips m
  on i.ip=m.ip
)

-- select count(distinct r.deviceid)
-- from ip_mobile_only_devices i join registered_mobile_only r
-- on i.device_id=r.deviceid

 select distinct s.platform, count(*) as num_events
 from (select top 5000 ip from mobile_only_ips) m
 join (select device_id, ts, platform,
               case when client_ip ilike '%.%' then client_ip
               when client_ipv6 is not NULL and len(client_ipv6)>20 then client_ipv6
               -- when client_ip ilike '%:%' and client_ipv6 is not NULL and len(client_ipv6)>20 then client_ipv6
               else NULL
               end as ip
        from server_impressions
        where ts>=dateadd('year', -1, CURRENT_DATE)
        and nvl(client_ip, client_ipv6) is not NULL
        and nvl(client_ip, client_ipv6) not in ('', ' ')
    ) s
on m.ip=s.ip
join scratch.ip_soft_match i on m.ip=i.ip
group by s.platform
