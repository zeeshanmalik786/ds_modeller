drop table if exists default.ds_modeller_iptv_throughput_actual_daily;
create table default.ds_modeller_iptv_throughput_actual_daily
select  concat(cmts_host_name, MAC_DOMAIN) as xref,
        cmts_host_name,
        MAC_DOMAIN,
        region,
        ((throughput_ninety_fifth_mbps_iptv*8)/1024/1024/3600) as throughput_ninety_fifth_mbps_iptv,
        iptv_distinct_device
from
(select percentile(Down_bytes,0.95) as throughput_ninety_fifth_mbps_iptv,
       count(distinct cm_mac_addr) as iptv_distinct_device,
       cmts_host_name, concat("md", substr(cmts_md_if_index, -2, 3)) as MAC_DOMAIN,
       RIGHT(
            cmts_host_name,
            CHARINDEX('.', (REVERSE(cmts_host_name))) - 1
          ) as region
from ium.mac_usage_fct
where event_date_utc BETWEEN (NOW() - INTERVAL 1 WEEK) and NOW() and state="2_WAVE3_MANAGED"
group by cmts_host_name, MAC_DOMAIN)