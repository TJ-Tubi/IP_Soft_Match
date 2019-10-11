set seed to .25;

create table scratch.ott_targeting_100719
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
        'tubitv-cox')
        then 'OTT'
        when us.app in ('tubitv-iphone','tubitv-ipad','tubitv-android','tubitv-firetablet','tubitv-android-samsung')
        then 'Mobile' else NULL end as platform
from (
  select device_id from server_impressions
--   where ts>=dateadd('week', -1, CURRENT_DATE)
  where ts between dateadd('year', -1, date('2019-10-09')) and date('2019-10-08')
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
  select distinct r.user_alias, r.deviceid
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
, preprocess_server_impressions as (
  select distinct device_id,
  case when client_ip ilike '%.%' then client_ip
              when client_ipv6 is not NULL and len(client_ipv6)>20 then client_ipv6
--               when client_ip ilike '%:%' and client_ipv6 is not NULL and len(client_ipv6)>20 then client_ipv6
              else NULL
         end as ip,

         case when platform IN ('tvos',
          'xbox360',
          'xboxone',
          'roku',
          'for-samsung',
          'samsung',
          'amazon',
          'sony',
          'ps4',
          'ps3',
          'tivo',
          'androidtv',
          'comcast',
          'cox')
        then 'OTT'
        when platform in ('iphone','ipad','android','firetablet','android-samsung')
        then 'Mobile' else 'UNKNOWN' end as platform

  from server_impressions
  where ts between dateadd('year', -1, date('2019-10-09')) and date('2019-10-08')
--   where ts>=dateadd('week', -1, CURRENT_DATE)
  and platform<>'Web'
  and nvl(client_ip, client_ipv6) is not NULL
  and nvl(client_ip, client_ipv6) not in ('', ' ')
  and geo_country_code='US'
)
, multidevice_ip_str_agg as (
  select distinct ip,
        listagg(distinct platform, ',') within group (order by device_id)
        over (partition by ip) as platform_str_agg

  from preprocess_server_impressions
)
, mobile_only_ips as (
  select *
  from multidevice_ip_str_agg
  where platform_str_agg='Mobile'
)
, ip_mobile_only_devices as (
  select distinct device_id
  from preprocess_server_impressions i join mobile_only_ips m
  on i.ip=m.ip
)
-- Below block generates the list
, user_list_full as (
select distinct r.user_alias as external_id
from ip_mobile_only_devices i join registered_mobile_only r
on i.device_id=r.deviceid
)

, cohortize as (
  select external_id, round(random()) as cohort
  from user_list_full
)

select * from cohortize

-- select cohort, count(distinct external_id)
-- from cohortize
-- group by cohort
);

-- List for upload to Braze
select external_id from scratch.ott_targeting_100719 where cohort = 1

/*
Below is a sanity check to make sure that the platform filtering worked (e.g. ips that only have a mobile device
active on it, no OTT
*/
--  select distinct s.platform
--                , count(*) as num_events
--  from (select ip from mobile_only_ips) m
--  join (select device_id, ts, platform,
--                case when client_ip ilike '%.%' then client_ip
--                when client_ipv6 is not NULL and len(client_ipv6)>20 then client_ipv6
--                -- when client_ip ilike '%:%' and client_ipv6 is not NULL and len(client_ipv6)>20 then client_ipv6
--                else NULL
--                end as ip
--         from server_impressions
--         where ts between dateadd('year', -1, date('2019-10-09')) and date('2019-10-08')
--         and nvl(client_ip, client_ipv6) is not NULL
--         and nvl(client_ip, client_ipv6) not in ('', ' ')
--         and platform in ('tvos',
--           'xbox360',
--           'xboxone',
--           'roku',
--           'for-samsung',
--           'samsung',
--           'amazon',
--           'sony',
--           'ps4',
--           'ps3',
--           'tivo',
--           'androidtv',
--           'comcast',
--           'cox'
-- --           ,'iphone','ipad','android','firetablet','android-samsung'
--           )
--     ) s
-- on m.ip=s.ip
-- group by s.platform