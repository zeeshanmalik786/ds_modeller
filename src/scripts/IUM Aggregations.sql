-- First Aggregation should be sum of all bytes down and up on Mac-domain level on hourly basis


-- Conversion of all down and up bytes in mbps
-- Second Aggregation should be max on daily level per macdomain - down and up.
-- Third aggregation should be AVG on weekly basis


drop table if exists default.ds_modeller_ium_4k_actual_daily;
create table default.ds_modeller_ium_4k_actual_daily
SELECT
  *,
  MAX(
    CASE
      WHEN state = "2_WAVE3_MANAGED" then 1_WK_Avg_DS
    end
  ) over (
    partition by cmts_host_name, md
  ) as md_iptv_sub_impact_mbps,
  MAX(
    CASE
      WHEN state = "2_WAVE3_MANAGED" then 1_WK_MAX_CM
    end
  ) over (
    partition by cmts_host_name, md
  ) as IPTV_subs_per_md,
  MAX(
    case
      when (
        state = "Valid"
        or state = "1_WAVE3_HSI"
      ) THEN 1_WK_MAX_CM
    end
  ) over (
    partition by cmts_host_name,
    md
  ) as total_rhsi_only_subs_ium --name this column properly

FROM
  (
  -- TAKE AVERAGE OVER THE DAILY'S (all 7 days)
  select
     Region,
     cmts_host_name,
     MAC_DOMAIN as md,
     concat(cmts_host_name, MAC_DOMAIN) as lookup,
     -- max of daily max cm's
     MAX(CM) as 1_Wk_MAX_CM,
     state,
     -- take average of max up and down over week
     max(up_mbps) as 1_Wk_Avg_US,
     max(down_mbps) as 1_Wk_Avg_DS,
     max(weighted_up_mbps) as 1_WK_Weighted_AVG_US,
     max(weighted_down_mbps) as 1_WK_Weighted_AVG_DS

--      average of last 4 weeks
from
(
-- TAKE MAX DAILY OF VALUES
select
     Region,
     cmts_host_name,
     --cmts_md_if_index,
     state,
     concat("md", substr(cmts_md_if_index, -2, 3)) as MAC_DOMAIN,
     MAX(up_mbps) as  up_mbps,
     MAX(down_mbps) as down_mbps,
     MAX(weighted_up_mbps) as weighted_up_mbps,
     MAX(weighted_down_mbps) as weighted_down_mbps,
     MAX(CM) as CM,
     event_date_utc
from
(
-- SUM UPBYTES AND DOWN BYTES HOURLY BASIS per cmts, mac_domain and state
select
          RIGHT(
            cmts_host_name,
            CHARINDEX('.', (REVERSE(cmts_host_name))) - 1
          ) as region,
          cmts_host_name,
          cmts_md_if_index,
          event_date_utc,
          hour(start_time_utc) as hour,
          SUM((up_bytes*8)/1024/1024/3600) as up_mbps,
          SUM((down_bytes*8)/1024/1024/3600) as down_mbps,
          SUM((up_bytes*8)/1024/1024/3600)/COUNT(DISTINCT(cm_mac_addr)) as weighted_up_mbps,
          SUM((up_bytes*8)/1024/1024/3600)/COUNT(DISTINCT(cm_mac_addr)) as weighted_down_mbps,
--           max on CMMAC level
          state,
          COUNT(Distinct(cm_mac_addr)) as CM
        from
          ium.mac_usage_fct
        where
          event_date_utc BETWEEN (NOW() - INTERVAL 1 WEEK)
--           throughput on CMMAC
          AND NOW()
          AND (
            state == "2_WAVE3_MANAGED"
            or state == "1_WAVE3_HSI"
            or state == "Valid"
          )
            and cmts_host_name is not null
        group by
          Region,
          cmts_host_name,
          cmts_md_if_index,
          state,
          event_date_utc,
          hour(start_time_utc)
          )
group by
          Region,
          cmts_host_name,
          --cmts_md_if_index,
          state,
          event_date_utc,
          MAC_DOMAIN)
group by
     Region,
     cmts_host_name,
     MAC_DOMAIN,
     state
     )
