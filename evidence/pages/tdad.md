---
title: TDAD Status
---

```sql vehicle_positions_by_month
  select
      date_trunc('month', day) as month,
      sum(count) as count
  from tdad.vehicle_positions_by_day
  group by month
  order by month asc
```

<BarChart
    data={vehicle_positions_by_month}
    x=month
    y=count
    title="Vehicle Positions by Month"
    yAxisTitle="Number of Vehicle Positions"
    xAxisTitle="Month"
/>
