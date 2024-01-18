/*Creating table "Deliveries" containing ball-ball data for IPL matches between 2008-2020*/
CREATE TABLE deliveries (
  id SERIAL PRIMARY KEY,
  inning INT,
  over INT,
  ball INT,
  batsman VARCHAR(50),
  non_striker VARCHAR(50),
  bowler VARCHAR(50),
  batsman_runs INT,
  extra_runs INT,
  total_runs INT,
  is_wicket BOOL,
  dismissal_kind VARCHAR(50),
  player_dismissed VARCHAR(50),
  fielder VARCHAR(50),
  extras_type VARCHAR(50),
  batting_team VARCHAR(50),
  bowling_team VARCHAR(50)
);

COMMENT ON COLUMN deliveries.id IS 'Unique match ID as per ESPN cricinfo';
COMMENT ON COLUMN deliveries.inning IS 'Inning number';
COMMENT ON COLUMN deliveries.over IS 'Over number in an inning';
COMMENT ON COLUMN deliveries.ball IS 'Ball number in an over';
COMMENT ON COLUMN deliveries.batsman IS 'Batsman on strike';
COMMENT ON COLUMN deliveries.non_striker IS 'Batsman at non-striker end';
COMMENT ON COLUMN deliveries.bowler IS 'Bowler';
COMMENT ON COLUMN deliveries.batsman_runs IS 'Runs off bat';
COMMENT ON COLUMN deliveries.extra_runs IS 'Extra runs';
COMMENT ON COLUMN deliveries.total_runs IS 'Total runs scored';
COMMENT ON COLUMN deliveries.is_wicket IS 'Is the delivery a wicket';
COMMENT ON COLUMN deliveries.dismissal_kind IS 'Type of dismissal';
COMMENT ON COLUMN deliveries.player_dismissed IS 'Player who got dismissed';
COMMENT ON COLUMN deliveries.fielder IS 'Fielder involved in dismissal';
COMMENT ON COLUMN deliveries.extras_type IS 'Type of extras';
COMMENT ON COLUMN deliveries.batting_team IS 'Batting team';
COMMENT ON COLUMN deliveries.bowling_team IS 'Bowling team';

/*Creating table matches with extension to above table*/
CREATE TABLE matches (
	id SERIAL PRIMARY KEY,
    city VARCHAR(50),
    date INT,
    player_of_match VARCHAR(50),
    venue VARCHAR(50),
    neutral_venue BOOL,
    team_1 VARCHAR(50),
    team_2 VARCHAR(50),
    toss_winner VARCHAR(50),
    toss_decision VARCHAR(50),
    winner VARCHAR(50),
    result VARCHAR(200),
    result_margin INT,
    eliminator BOOL,
    method BOOL,
    umpire_1 VARCHAR(50),
    umpire_2 VARCHAR(50)
);

COMMENT ON COLUMN matches.id IS 'Unique match ID as per ESPN cricinfo';
COMMENT ON COLUMN matches.city IS 'City in which the stadium is located';
COMMENT ON COLUMN matches.date IS 'Date on which the match is held';
COMMENT ON COLUMN matches.player_of_match IS 'Player awarded with the best performance';
COMMENT ON COLUMN matches.venue IS 'Stadium name';
COMMENT ON COLUMN matches.neutral_venue IS 'Is the venue neutral to playing teams';
COMMENT ON COLUMN matches.team_1 IS 'Team 1';
COMMENT ON COLUMN matches.team_2 IS 'Team 2';
COMMENT ON COLUMN matches.toss_winner IS 'Team who won the toss';
COMMENT ON COLUMN matches.toss_decision IS 'Decision of the toss winner';
COMMENT ON COLUMN matches.winner IS 'Match-winning team';
COMMENT ON COLUMN matches.result IS 'Result based on victory by runs or by wickets';
COMMENT ON COLUMN matches.result_margin IS 'Margin of victory in terms of wickets or runs';
COMMENT ON COLUMN matches.eliminator IS 'Was a super over bowled or not';
COMMENT ON COLUMN matches.method IS 'Was the DL method applied';
COMMENT ON COLUMN matches.umpire_1 IS 'First umpire';
COMMENT ON COLUMN matches.umpire_2 IS 'Second umpire';

select * from deliveries;
--Altering the datatype of matches.date column from INT to data
ALTER TABLE matches
ALTER COLUMN date TYPE DATE USING TO_DATE(date::text, 'DD-MM-YYYY');

--Altering the columns BOOL type with VARCHAR to handle NULL values
ALTER TABLE matches
ALTER COLUMN eliminator TYPE VARCHAR(50),
ALTER COLUMN method TYPE VARCHAR(50);

ALTER TABLE matches
ALTER COLUMN venue TYPE VARCHAR(200);

--pt->3 Copying csv data to sql table 'matches'
COPY matches/*(id,city,date,player_of_match,venue,neutral_venue,team_1,team_2,toss_winner,toss_decision,winner,result,result_margin,eliminator,method,umpire_1,umpire_2)*/
FROM 'C:\Program Files\PostgreSQL\12\data\data\IPL_matches.csv' 
DELIMITER ',' CSV HEADER;

--pt->4 Copying csv data to sql table 'deliveries' changing constraint p_key

ALTER TABLE deliveries
DROP CONSTRAINT IF EXISTS deliveries_pkey;

COPY deliveries
FROM 'C:\Program Files\PostgreSQL\12\data\data\IPL_BALL.csv' 
DELIMITER ',' CSV HEADER;

--pt->5.Select the top 20 rows of the deliveries table after ordering them by id, inning, over, ball in ascending order
COPY (
	SELECT * FROM deliveries
	ORDER BY id,inning,over,ball
	LIMIT 20 
) TO 'C:\Program Files\PostgreSQL\12\data\data\Query_5.csv' WITH CSV HEADER;

--pt->6.Select the top 20 rows of the matches table.
COPY (
	SELECT * FROM matches
	LIMIT 20 
) TO 'C:\Program Files\PostgreSQL\12\data\data\Query_6.csv' WITH CSV HEADER;
--pt->7.Fetch data of all the matches played on 2nd May 2013 from the matches table..
COPY (
	SELECT * FROM matches
	WHERE date='02-05-2013'
) TO 'C:\Program Files\PostgreSQL\12\data\data\Query_7.csv' WITH CSV HEADER;
--pt->8.Fetch data of all the matches where the result mode is ‘runs’ and margin of victory is more than 100 runs
COPY (
	SELECT * FROM matches
	WHERE result='runs' and result_margin > 100
) TO 'C:\Program Files\PostgreSQL\12\data\data\Query_8.csv' WITH CSV HEADER;
--pt->9.Fetch data of all the matches where the final scores of both teams tied and order it in descending order of the date.
COPY (
	SELECT * FROM matches
	WHERE result='tie'
	ORDER BY date desc
) TO 'C:\Program Files\PostgreSQL\12\data\data\Query_9.csv' WITH CSV HEADER;
--pt->10.Get the count of cities that have hosted an IPL match
COPY (
	SELECT COUNT(DISTINCT city) FROM matches
) TO 'C:\Program Files\PostgreSQL\12\data\data\Query_10.csv' WITH CSV HEADER;

/*pt->11.Create table deliveries_v02 with all the columns of the table ‘deliveries’ 
and an additional column ball_result containing values boundary, dot or 
other depending on the total_run (boundary for >= 4, dot for 0 and other for any other number)
(Hint 1 : CASE WHEN statement is used to get condition based results)
(Hint 2: To convert the output data of select statement into a table, you can use a subquery. 
Create table table_name as [entire select statement].*/
COPY (
	CREATE TABLE deliveries_v02 as
	SELECT *,
		CASE 
			WHEN total_run >=4 THEN 'boundary'
			WHEN total_run=0 THEN 'dot'
			ELSE 'Other'
		END AS ball_result	
	FROM deliveries
) TO 'C:\Program Files\PostgreSQL\12\data\data\Query_11.csv' WITH CSV HEADER;
/*pt->12.fetch the total number of boundaries and dot balls from the deliveries_v02 table*/
COPY(
	SELECT COUNT(*) as total_bound_dot
	FROM deliveries_v02
	WHERE ball_result in ('boundary','dot')
	) to'C:\Program Files\PostgreSQL\12\data\data\Query_12.csv' WITH CSV HEADER;
/*pt->13.fetch the total number of boundaries scored by each team from the deliveries_v02
table and order it in descending order of the number of boundaries scored.*/
COPY(
	SELECT batting_team, COUNT(*) as total_boundary
	FROM deliveries_v02
	WHERE ball_result='boundary'
	GROUP BY batting_team
	ORDER BY total_boundary desc
	) to'C:\Program Files\PostgreSQL\12\data\data\Query_13a.csv' WITH CSV HEADER;
/*pt->14.fetch the total number of dot balls bowled by each team and order it in descending order
of the total number of dot balls bowled*/
COPY(
	SELECT batting_team, COUNT(*) as total_dots
	FROM deliveries_v02
	WHERE ball_result='dot'
	GROUP BY batting_team
	ORDER BY total_dots desc
	) to'C:\Program Files\PostgreSQL\12\data\data\Query_14.csv' WITH CSV HEADER;
/*pt->15.fetch the total number of dismissals by dismissal kinds where dismissal kind is not NA*/
COPY(
	SELECT dismissal_kind, COUNT(*) AS total_dismissal
	FROM deliveries_v02
	WHERE dismissal_kind!= 'NULL'
	GROUP BY dismissal_kind
	ORDER BY total_dismissal desc
) to 'C:\Program Files\PostgreSQL\12\data\data\Query_15.csv' WITH CSV HEADER;
/*pt->16.Get the top 5 bowlers who conceded maximum extra runs from the deliveries table*/
COPY(
	SELECT bowler, SUM(extra_runs) AS top_conceded_extra
	FROM deliveries
	GROUP BY bowler
	ORDER BY top_conceded_extra desc
	LIMIT 5
) to 'C:\Program Files\PostgreSQL\12\data\data\Query_16.csv' WITH CSV HEADER;
/*pt->17.Create a table named deliveries_v03 with all the columns of deliveries_v02 table 
and two additional column (named venue and match_date) of venue and date from table matches*/
CREATE TABLE deliveries_v03 AS
	SELECT d2.*, m.venue as match_venue, m.date as match_date
	FROM deliveries_v02 AS d2
	INNER JOIN matches as m
	ON d2.id=m.id;
COPY deliveries_v03 to 'C:\Program Files\PostgreSQL\12\data\data\Query_17.csv' WITH CSV HEADER;
/*pt->18.fetch the total runs scored for each venue and order it in the descending order of total runs scored*/
COPY (
	SELECT match_venue, SUM(total_runs) as totalruns
	FROM deliveries_v03
	GROUP BY match_venue
	ORDER BY totalruns desc
	) to 'C:\Program Files\PostgreSQL\12\data\data\Query_18.csv' WITH CSV HEADER;
/*pt->19.fetch the year-wise total runs scored at Eden Gardens and order it 
in the descending order of total runs scored.*/
COPY (
	SELECT EXTRACT(YEAR FROM match_date) AS year, sum(total_runs) AS totalruns
	FROM deliveries_v03
	WHERE match_venue='Eden Gardens'
	GROUP BY year
	ORDER BY totalruns
	) to 'C:\Program Files\PostgreSQL\12\data\data\Query_19.csv' WITH CSV HEADER;
/*pt->20.Get unique team1 names from the matches table, you will notice that there are two entries 
for Rising Pune Supergiant one with Rising Pune Supergiant and another one with Rising Pune Supergiants.  
Your task is to create a matches_corrected table with two additional columns team1_corr and team2_corr 
containing team names with replacing Rising Pune Supergiants with Rising Pune Supergiant.*/
CREATE TABLE matches_corrected AS
	SELECT *,CASE 
				WHEN team_1='Rising Pune Supergiants' 
				THEN 'Rising Pune Supergiant' 
				ELSE team_1 
			 END AS team1_corr,
			 CASE 
			 	WHEN team_2='Rising Pune Supergiants' 
				THEN 'Rising Pune Supergiant' 
				ELSE team_2 
			 END AS team2_corr
	FROM matches;
COPY matches_corrected TO 'C:\Program Files\PostgreSQL\12\data\data\Query_20.csv' WITH CSV HEADER;
/*pt->21.Create a new table deliveries_v04 with the first column as ball_id containing information of match_id, inning, over and ball separated by ‘-’ 
(For ex. 335982-1-0-1 match_id-inning-over-ball) and rest of the columns same as deliveries_v03)*/
CREATE TABLE deliveries_v04 AS
SELECT
    d3.id || '-' || d3.inning || '-' || d3.over || '-' || d3.ball AS ball_id,
    d3.*    -- select all columns from deliveries_v03
FROM
    deliveries_v03 AS d3;
COPY deliveries_v04 TO 'C:\Program Files\PostgreSQL\12\data\data\Query_21.csv' WITH CSV HEADER;
/*pt->22.Compare the total count of rows and total count of distinct ball_id in deliveries_v04*/
COPY (
	SELECT COUNT(*) AS TOTAL_ROWS, COUNT (DISTINCT ball_id) AS distinc_ballid FROM deliveries_v04
) TO 'C:\Program Files\PostgreSQL\12\data\data\Query_22.csv' WITH CSV HEADER;
/*PT->23.Create table deliveries_v05 with all columns of deliveries_v04 and an additional column
for row number partition over ball_id.*/
CREATE TABLE deliveries_v05 AS
SELECT d4.*, ROW_NUMBER() OVER (PARTITION BY ball_id) AS r_num
FROM deliveries_v04 AS d4;
COPY deliveries_v05 TO 'C:\Program Files\PostgreSQL\12\data\data\Query_23.csv' WITH CSV HEADER;
/*pt->24.Use the r_num created in deliveries_v05 to identify instances where ball_id is repeating.*/
SELECT ball_id, COUNT(*) AS repetitions
FROM deliveries_v05
GROUP BY ball_id
HAVING COUNT(*) > 1;
COPY (select * from deliveries_v05 where r_num=2) TO 'C:\Program Files\PostgreSQL\12\data\data\Query_24.csv' WITH CSV HEADER;
/*pt->25.Use subqueries to fetch data of all the ball_id which are repeating*/
SELECT * FROM deliveries_v05 WHERE ball_id in (select BALL_ID from deliveries_v05 WHERE r_num=2);


	
					
	




	



















