/*
This is the query that actually writes the table; should live in github, not a console scratch
*/

drop table if exists scratch.ip_soft_match;

create table scratch.ip_soft_match
  distkey (device_id)
  sortkey (client_ip, first_activity_ts)
as (
/*
device_ip_activity groups by deviceid and ip; hence, it's a tally of devices and the IPs that they have been
active on at any point in our historical data
Each row in server_impressions is counted as activity
The latest timestamp of a device and IP pairing are also tallied
*/
WITH device_ip_activity AS (
  SELECT device_id,
         COALESCE(client_ipv6,client_ip) AS client_ip,
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
  and client_ip is not NULL
  and client_ip<>''
  /*
  Filter below is only enabled for debugging purposes
  */
--   WHERE s.ts > dateadd('day', -3, current_date)
  GROUP BY 1,2
)
/*
rankings aggregates by device_id via the rank() window function, and orders IP activeness using
the number of server impressions first and recency second (to break ties in case of equal
activity on multiple IPs)
ip_rank represents the activity level of a device on all the IPs it has pinged the ad server from
E.g. my iphone has pinged the ad server 100 times at home, and 50 times on tubi wifi. Home would be ranked
1 and Tubi-wifi would be ranked 2
*/
, rankings as (
  SELECT device_id,
  client_ip,
  RANK() OVER (partition by device_id order by activity desc, latest_ts desc) as ip_rank
  FROM device_ip_activity
)
/*
device_per_ip tallies the number of devices that were active on the same IP
There is no specific time period involved, as the cutoff was chosen offline
*/
, devices_per_ip as (
  select client_ip, count(distinct device_id) as num_devices
  from rankings
  where ip_rank = 1
  group by client_ip
  having num_devices<=5
)
/*
ip_device_pairs is really only a filter step
*/
, ip_device_pairs as (
  select r.client_ip, r.device_id
  from rankings r
  join (select client_ip from devices_per_ip) d
  on r.client_ip=d.client_ip
--   where ip_rank=1
  where ip_rank<=10
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
  select i.client_ip,
         i.device_id,
         min(s.ts) as first_activity_ts,
         max(s.ts) as last_activity_ts,
         max(a.app) as app,
         max(a.platform) as platform,
         rank() over (partition by i.client_ip order by first_activity_ts asc) as device_active_order
  from ip_device_pairs i join server_impressions s
  on i.client_ip=s.client_ip and i.device_id=s.device_id
  join scratch.app_info a on s.platform=substring(app, charindex('-', app)+1, len(app)-charindex('-', app))
  group by i.client_ip, i.device_id
)

select * from activity_ts
);
GRANT select ON scratch.ip_soft_match to periscope_readonly; commit;