drop table if exists scratch.ip_soft_match;

create table scratch.ip_soft_match
  distkey (device_id)
  sortkey (ip, first_activity_ts)
as (
/*
device_ip_activity groups by deviceid and ip; hence, it's a tally of devices and the IPs that they have been
active on at any point in our historical data
Each row in server_impressions is counted as activity
The latest timestamp of a device and IP pairing are also tallied
*/
WITH device_ip_activity AS (
  SELECT device_id,
         case when client_ip ilike '%.%' then client_ip
              when client_ip ilike '%:%' and client_ipv6 is not NULL and len(client_ipv6)>20 then client_ipv6
              else NULL
         end as ip,
         /*
         production solution should join with user_alias_first_country_v
         this is an expensive operation however so eda will just use the field in user_signon
         */
         count(*) as activity, max(s.ts) as latest_ts
  FROM server_impressions s
  join derived.user_signon u on s.device_id=u.deviceid
  and filter_tag=0
  and u.signup_countries='US'
  and device_id not in (0, '')
  and device_id is not NULL
  and nvl(client_ip, client_ipv6) is not NULL
  and nvl(client_ip, client_ipv6) not in ('', ' ')
  /*
  Filter below is only enabled for debugging purposes
  */
--   WHERE s.ts > dateadd('day', -3, current_date)
  GROUP BY 1,2
  having ip is not NULL
)
/*
rankings aggregates by device_id via the rank() window function, and orders IP activeness using
the number of server impressions first and recency second (to break ties in case of equal
activity on multiple IPs)

In other words, the rankings CTE helps to assign a primary (or a set of primary) IP addresses to each
deviceid based on activity.

ip_rank represents the activity level of a device on all the IPs it has pinged the ad server from
E.g. my iphone has pinged the ad server 100 times at home, and 50 times on tubi wifi. Home would be ranked
1 and Tubi-wifi would be ranked 2
*/

, rankings as (
  SELECT device_id,
  ip,
  RANK() OVER (partition by device_id order by activity desc, latest_ts desc) as ip_rank
  FROM device_ip_activity
)

/*
device_per_ip tallies the number of devices that were active on the same IP
If an IP has too many devices, it is excluded as the dataset would become far too noisy otherwise

Todo: Once the probabilistic model has been developed, we may be able to loosen this constraint
*/

, devices_per_ip as (
  select ip, count(distinct device_id) as num_devices
  from rankings
  group by ip
  having num_devices<=5
)

/*
ip_device_pairs is a filter step that excludes the ips filtered out in devices_per_ip
*/
, ip_device_pairs as (
  select r.ip, r.device_id
  from rankings r
  join (select ip from devices_per_ip) d -- remove from dataset the ips that have too many devices active on them
  on r.ip=d.ip
  where ip_rank<=10 -- only include ips that rank at least top 10 on a given device
),

/*
activity_ts tallies the first and last activities of a particular device on a particular IP
device_active_order is the order at which devices pinged a particular IP
For instance, if Davide's Android pings Tubi-wifi in 2018 for the first time, and my iPhone pings Tubi-wifi in 2019
for the first time, then Davide's Android would be ranked 1, and my iPhone would be ranked 2.
Filtering on this rank has the effect of limiting whether or not we consider only the first devices on an IP to be
eligible for 'migration'. If a user was active on OTT first, then mobile, then ott, should a mobile to OTT migration
be counted?
*/
activity_ts as (
  select i.ip,
         i.device_id,
         min(s.ts) as first_activity_ts,
         max(s.ts) as last_activity_ts,
         max(a.app) as app,
         max(a.platform) as platform,
         rank() over (partition by i.ip order by first_activity_ts asc) as device_active_order
  from ip_device_pairs i join server_impressions s
  on i.ip=s.client_ip and i.device_id=s.device_id
  join scratch.app_info a on s.platform=substring(app, charindex('-', app)+1, len(app)-charindex('-', app))
  group by i.ip, i.device_id
)

select * from activity_ts
);
GRANT select ON scratch.ip_soft_match to periscope_readonly; commit;