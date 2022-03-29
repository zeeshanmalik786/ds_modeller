drop table if exists default.ds_modeller_ccap_daily ;
create table default.ds_modeller_ccap_daily
-- ASK ABOUT NRM_TABLE FOR Fwd's
select
RIGHT(
        A.cmts_host_name,
        CHARINDEX('.', (REVERSE(A.cmts_host_name))) - 1
      ) as region,
       A.cmts_host_name,
       A.MAC_DOMAIN,
       A.OFDM_GROUP_OUT_TRAFFIC_95TH_AVG,
       A.SCQAM_GROUP_OUT_TRAFFIC_95TH_AVG,
       A.Total_DS_Capacity_Mbps_SCQAM_OFDM,
       (OFDM_GROUP_OUT_TRAFFIC_95TH_AVG + SCQAM_GROUP_OUT_TRAFFIC_95TH_AVG) as current_md_util_mbps,
       ct_fwd_seg_in_md

FROM
(
select cmts_host_name,
       cmts_md_if_index,
       SUM(tot) as Total_DS_Capacity_Mbps_SCQAM_OFDM,
       concat("md",substr(cmts_md_if_index,-2,3)) as MAC_DOMAIN,
       COLLECT_SET(OFDMA_GROUP_OUT_TRAFFIC_95TH)[0]/1000000 AS OFDM_GROUP_OUT_TRAFFIC_95TH_AVG,
       COLLECT_SET(SCQAM_GROUP_OUT_TRAFFIC_95TH)[0]/1000000 AS SCQAM_GROUP_OUT_TRAFFIC_95TH_AVG, date_key
from
(
select  cmts_host_name,
        cmts_md_if_index,
        SUM(capacity_bps / 1000000) as tot,
        ds_throughput_bps_95,
        is_md_lvl_flg,time_flag,
        date_key,
        CASE WHEN is_md_lvl_flg == "O" then ds_throughput_bps_95  end as OFDMA_GROUP_OUT_TRAFFIC_95TH,
        CASE WHEN is_md_lvl_flg == "S" then ds_throughput_bps_95 end as SCQAM_GROUP_OUT_TRAFFIC_95TH
FROM hem.ccap_ds_md_util_daily_fct
WHERE is_md_lvl_flg != "P" and
      time_flag == "A" and
      -- previous day
      date_key=date_sub(current_date,3) and
      cmts_host_name like "cmts%" -- removed non cmts in host names
      and cmts_host_name is not null

group by cmts_host_name,
         cmts_md_if_index,
         is_md_lvl_flg,
         time_flag,
         date_key,
          ds_throughput_bps_95
)
group by cmts_host_name,
         cmts_md_if_index,
         date_key) as A
 left join (
          select
            cmts,
            concat("md", substr(mac_domain, -2, 3)) as MAC_DOMAIN,
            count(distinct(fwd_seg)) as ct_fwd_seg_in_md
          from
            hem.nrm_rhsi_topology
          where
            received_date = date_sub(current_date, 3) --           one day of data two days back
          group by
            cmts,
            mac_domain
        ) as nrm_fwd_seg on A.MAC_DOMAIN = nrm_fwd_seg.MAC_DOMAIN AND A.cmts_host_name = nrm_fwd_seg.cmts

