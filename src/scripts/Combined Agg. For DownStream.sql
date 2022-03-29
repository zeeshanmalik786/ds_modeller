drop table if exists default.ds_modeller_down_byte_daily_staging;
create table default.ds_modeller_down_byte_daily_staging
select
  distinct concat(tbl2.cmts_host_name, tbl2.MAC_DOMAIN) as xref,
  coalesce(table3.final_growth_rate + 1, 'N/A') as regional_rhsi_cmgr,
  coalesce(table3.t3_4 + 0.01, 'N/A') as regional_t5_trigger,
  coalesce(table3.t1, 'N/A') as regional_t1_trigger,
  coalesce(table3.t3_4, 'N/A') as regional_t3_4_trigger,
  coalesce(sam34.CBU, 'N/A') as CBU,
  coalesce(sam34.eng_region) as eng_region,
  coalesce(sam34.PHUB, 'N/A') as PHUB,
  coalesce(tbl2.cmts_host_name, 'N/A') as cmts_name,
  coalesce(tbl2.MAC_DOMAIN, 'N/A') as mac_domain,
  coalesce(tbl2.Total_DS_Capacity_Mbps_SCQAM_OFDM, 'N\A') as total_ds_capacity_in_mbps_scqam_ofdm,
  coalesce(tbl2.current_md_util_mbps, 'N\A') as current_md_util_mbps,
  coalesce(tbl2.ct_fwd_seg_in_md, 'N/A') as current_ct_fwds_in_md,
  coalesce(tbl1.total_rhsi_only_subs_ium, 'N/A') AS total_rhsi_only_subs_ium,
  coalesce((SUM(
  case
  when state = "2_WAVE3_MANAGED"  THEN tbl1.1_Wk_Avg_DS end) over (partition by sam34.PHUB)/ SUM(case when state="2_WAVE3_MANAGED" THEN     1_WK_MAX_CM end) over (partition by sam34.PHUB)),
  'N/A') as iptv_throughput_by_phub,
  coalesce(
    tbl1.total_rhsi_only_subs_ium / SUM(
      case
        when (
          state = "Valid"
          or state = "1_WAVE3_HSI"
        ) THEN 1_WK_MAX_CM
      end
    ) over (partition by sam34.PHUB),
    'N/A'
  ) * 100 as md_perc_of_site,
  coalesce(tbl1.md_iptv_sub_impact_mbps, 'N/A') as md_iptv_sub_impact_mbps,
  coalesce(tbl1.IPTV_subs_per_md, 'N/A') as IPTV_subs_per_md,
  coalesce(
    tbl1.md_iptv_sub_impact_mbps / tbl2.Total_DS_Capacity_Mbps_SCQAM_OFDM,
    'N/A'
  ) as iptv_perc_of_total_cap,
  coalesce(
    case
      when (
        tbl2.current_md_util_mbps - tbl1.md_iptv_sub_impact_mbps >= 1
      ) then (
        tbl2.current_md_util_mbps - tbl1.md_iptv_sub_impact_mbps
      )
      else 1
    end,
    'N/A'
  ) as current_md_rhsi_only_mbps,
  coalesce(
    tbl2.current_md_util_mbps / tbl2.Total_DS_Capacity_Mbps_SCQAM_OFDM,
    'N/A'
  ) as current_perc_util_per_ium,
  0.05 as perc_traffic_with_caps,
  0.30 as perc_increase_in_traffic_with_no_caps,
  0.195 as iptv_savings,
  0.13 as reduction_of_Gb_suspension,
  0.58 as Gb_concurrency
from
  ds_modeller_ccap_daily tbl2
  left join ds_modeller_ium_4k_actual_daily tbl1 on (
    tbl1.cmts_host_name = tbl2.cmts_host_name
    and tbl1.md = tbl2.MAC_DOMAIN
  )
  left join (
    select
      CBU,
      CMTS_NAME as cmts_host_name,
      eng_region as eng_region,
      PHUB
    FROM
      cr_tim.sam34_us_ds
  ) AS sam34 ON tbl2.cmts_host_name = sam34.cmts_host_name
  left join(
    select
      PHUB,
      final_growth_rate,
      Constant_value /(final_growth_rate + 1) /(final_growth_rate + 1) /(final_growth_rate + 1) as t1,
      Constant_value /(final_growth_rate + 1) /(final_growth_rate + 1) /(final_growth_rate + 1) /(final_growth_rate + 1) /(final_growth_rate + 1) /(final_growth_rate + 1) as t3_4
    from(
        select
          *,
          case
            when down_grow_rate < down_grow_rate then down_grow_rate
            else down_grow_rate
          end as final_growth_rate
        from
          (
            select
              PHUB,
              down_grow_rate,
              avg_gowth_rate,
              0.75 as Constant_value
            from
              ds_modeller_growth_rate_daily
              join (
                select
                  avg(down_grow_rate) as avg_gowth_rate
                from
                  ds_modeller_growth_rate_daily
              )
          )
      )
  ) as table3 on sam34.PHUB = table3.PHUB