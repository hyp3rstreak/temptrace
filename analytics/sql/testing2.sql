select count(*) from weather_hourly

select l.name as city, l.state, l.country, min(h.observation_time),round(max(h.temperature_2m)::numeric,2) as max_temperature, count(h.temperature_2m) as data_points
from weather_hourly as h
join locations as l
on h.location_id = l.id
group by city, l.state, l.country
order by data_points desc;

select * from locations

select count(h.temperature_2m)
from weather_hourly as h
where location_id = 14