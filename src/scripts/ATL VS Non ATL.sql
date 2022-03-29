select region, max(down_grow_rate) as avg_down_growth_rate, max(up_grow_rate) as avg_up_growth_rate
from
(
select growth_rate.PHUB, down_grow_rate, up_grow_rate, CBU,
case when CBU = 'GTA' or CBU ='ONT' then 'non_Atlantic'
else 'Atlantic'
end as region

from default.ds_modeller_growth_rate_daily as growth_rate
join
(select distinct CBU, PHUB from cr_tim.sam34_us_ds) as sam34
on growth_rate.PHUB = sam34.PHUB)
group by region