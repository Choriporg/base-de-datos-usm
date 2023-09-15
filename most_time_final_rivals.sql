SELECT winners , runnerUp, count(*) as count
FROM (select YEAR_PLAYED as year1, team as winners from mundial where place=1) as table1,
(select YEAR_PLAYED as year2,team as runnerUp from mundial where place=2) as table2
WHERE year1=year2
group by winners, runnerUp
order by count desc
