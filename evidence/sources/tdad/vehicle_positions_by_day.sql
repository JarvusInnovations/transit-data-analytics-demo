SELECT
  date_trunc(dt, day) as day,
  COUNT(*) AS count
FROM
  `external_transit_data.gtfs_rt__vehicle_positions`
GROUP BY
  day
ORDER BY
  day ASC
