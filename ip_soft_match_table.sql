drop table if exists scratch.ip_soft_match;
create table scratch.ip_soft_match
  distkey (device_id)
  sortkey (device_id, first_activity_ts)
as (
  WITH device_ip_activity AS (
  SELECT device_id, COALESCE(client_ipv6,client_ip) AS client_ip,
         count(*) as activity
  FROM server_impressions
--   WHERE ts > dateadd('day', -3, current_date)
  GROUP BY 1,2
)
, rankings as (
  SELECT device_id,
  client_ip,
  RANK() OVER (partition by device_id order by activity DESC) as ip_rank
  FROM device_ip_activity
)
, devices_per_ip as (
  select client_ip, count(distinct device_id) as num_devices
  from rankings
  where ip_rank = 1
  group by client_ip
  having num_devices<=10
)
, ip_device_pairs as (
  select r.client_ip, r.device_id
  from rankings r
  join (select client_ip from devices_per_ip) d
  on r.client_ip=d.client_ip
  where ip_rank=1
),
activity_ts as (
  select i.client_ip,
         i.device_id,
         min(s.ts) as first_activity_ts,
         max(s.ts) as last_activity_ts,
         max(a.app) as app,
         max(a.platform) as platform
  from ip_device_pairs i join server_impressions s
  on i.client_ip=s.client_ip and i.device_id=s.device_id
  join scratch.app_info a on s.platform=SPLIT_PART(a.app, '-', 2)
  group by i.client_ip, i.device_id
)

select * from activity_ts
);
GRANT select ON scratch.ip_soft_match to periscope_readonly; commit;