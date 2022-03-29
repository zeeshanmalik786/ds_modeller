drop table if exists ds_modeller_growth_rate_daily;
create table ds_modeller_growth_rate_daily
select
  PHUB,
  down_grow_rate,
  up_grow_rate
from
  (
    select
      PHUB,
      order_month,
      average_down,
      lag(average_down) over (
        partition by PHUB
        order by
          order_month
      ) as previous_average_down,
      average_up,
      lag(average_up) over (
        partition by PHUB
        order by
          order_month
      ) as previous_average_up,
      power(
        (
          average_down / lag(average_down) over (
            partition by PHUB
            order by
              order_month
          )
        ),
        1 / 12
      ) -1 as down_grow_rate,
      power(
        (
          average_up / lag(average_up) over (
            partition by PHUB
            order by
              order_month
          )
        ),
        1 / 12
      ) - 1 as up_grow_rate
    from(
        select
          PHUB,
          order_month,
          max(average_down) as average_down,
          max(average_up) as average_up
        from
          (
            select
              cmts_host_name,
              count(cmts_host_name) as count,
              round(max(average_down), 2) as average_down,
              round(max(average_up), 2) as average_up,
              order_month
            from(
                select
                  cmts_host_name,
                  count(cmts_host_name),
                  sum(down_MBPs) as sum_down,
                  max(down_MBPs) as average_down,
                  sum(up_MBPs) as sum_up,
                  max(up_MBPs) as average_up,
                  date,
                  order_month
                from(
                    select
                      cmts_host_name,
                      down_bytes / 1000000 as down_MBPs,
                      up_bytes / 1000000 as up_MBPs,
                      state,
                      event_date_utc as date,
                      case
                        when event_date_utc <= current_date -1
                        and event_date_utc >= current_date -31 then 2
                        else 1
                      end as order_month
                    from
                      ium.mac_usage_fct
                    where
                      (
                        event_date_utc <= current_date -1
                        and event_date_utc >= current_date - 31
                      )
                      or (
                        event_date_utc <= current_date - 366
                        and event_date_utc >= current_date - 397
                      )
                  )
                group by
                  cmts_host_name,
                  date,
                  order_month
              )
            group by
              cmts_host_name,
              order_month
          ) as ori
          join (
            select
              CBU,
              PHUB,
              CMTS_NAME
            FROM
              cr_tim.sam34_us_ds
          ) AS sam34 ON ori.cmts_host_name = sam34.CMTS_NAME
        group by
          PHUB,
          order_month
      )
  )
where
  order_month = 2