drop table if exists default.ds_modeller_down_byte_daily;
create table default.ds_modeller_down_byte_daily
select A.xref as xref,
       A.cmts_host_name,
       A.MAC_DOMAIN,
       A.region,
       A.throughput_ninety_fifth_mbps_iptv,
       B.regional_rhsi_cmgr,
       B.regional_t5_trigger,
       B.regional_t1_trigger,
       B.regional_t3_4_trigger,
       B.CBU,
       B.eng_region,
       B.PHUB,
       B.cmts_name,
       B.total_ds_capacity_in_mbps_scqam_ofdm,
       B.current_md_util_mbps,
       B.current_ct_fwds_in_md,
       B.total_rhsi_only_subs_ium,
       B.iptv_throughput_by_phub,
       B.md_perc_of_site,
       B.md_iptv_sub_impact_mbps,
       B.IPTV_subs_per_md,
       B.iptv_perc_of_total_cap,
       B.current_md_rhsi_only_mbps,
       B.current_perc_util_per_ium
       from
(select xref, cmts_host_name, MAC_DOMAIN, region, throughput_ninety_fifth_mbps_iptv from default.ds_modeller_iptv_throughput_actual_daily) AS A
JOIN
(select * from default.ds_modeller_down_byte_daily_staging) AS B
ON A.xref=B.xref and A.cmts_host_name=B.cmts_name and A.MAC_DOMAIN=B.MAC_DOMAIN