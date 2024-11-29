-- list all the existing data 
SELECT * FROM player_seasons;

-- creating user defined struct data type
-- type is created for always changing attributes
CREATE TYPE season_stats_r AS (
				season INTEGER,
				gp INTEGER,
				pts REAL,
				reb REAL,
				ast REAL
);

CREATE TYPE scoring_class_r AS ENUM('star','good','average','bad');

-- creating table for identifiers & attributes as an array of created type
-- DROP table players_r;

CREATE TABLE players_r(
	player_name TEXT,
	height TEXT,
	college TEXT,
	country TEXT,
	draft_year TEXT,
	draft_round TEXT,
	draft_number TEXT,
	season_stats_r season_stats_r[],
	scoring_class_r scoring_class_r,
	years_since_last_season INTEGER,
	current_season INTEGER,
	PRIMARY KEY(player_name,current_season)
);

-- check the min year to start cumilative data retrival
SELECT MIN(season) FROM player_seasons;

-- start cumilative data retrival along with full outer join
-- seed query starts yesturday current season from min(season)-1 == 1995, today = min(season) == 1996 and increments them
TRUNCATE table players_r;
INSERT INTO players_r
WITH yesturday AS(
	SELECT * FROM players_r
	WHERE current_season = 2000
),
today AS (
	SELECT * FROM player_seasons
	WHERE season = 2001
)
SELECT  
	COALESCE(t.player_name,y.player_name) AS player_name,
	COALESCE(t.height,y.height) AS height,
	COALESCE(t.college,y.college) AS college,
	COALESCE(t.country,y.country) AS country,
	COALESCE(t.draft_year,y.draft_year) AS draft_year,
	COALESCE(t.draft_round,y.draft_round) AS draft_round,
	COALESCE(t.draft_number,y.draft_number) AS draft_number,
	CASE WHEN y.season_stats_r IS NULL
			THEN ARRAY[ROW(
						t.season,
						t.gp,
						t.pts,
						t.reb,
						t.ast
			)::season_stats_r]
		WHEN t.season IS NOT NULL
			THEN y.season_stats_r || ARRAY[ROW(
						t.season,
						t.gp,
						t.pts,
						t.reb,
						t.ast
			)::season_stats_r]
		ELSE y.season_stats_r END AS season_stats,

	CASE WHEN t.season IS NOT NULL THEN 
			CASE WHEN t.pts > 20 THEN 'star'
				 WHEN t.pts > 15 THEN 'good'
				 WHEN t.pts > 10 THEN 'average'
			ELSE 'bad' 
			END :: scoring_class_r
		 ELSE y.scoring_class_r
		 END AS scoring_class_r,

	CASE WHEN t.season IS NOT NULL THEN 0
		 ELSE y.years_since_last_season + 1 
		 END as years_since_last_season,
	COALESCE(t.season, y.current_season + 1) AS current_season
FROM today t FULL OUTER JOIN yesturday y
ON t.player_name = y.player_name;

-- checking whether data is inserted as expected 
SELECT * FROM players_r;


-- here as we change the yesturday, today yrs and insert the records in players_r table
-- it appends the data to existing one on players_r table.
-- so we can just opt only the current season by current_season column which holds all 
-- its previous years data too in season_stats_r array
SELECT * FROM players_r
WHERE current_season = 2001 AND player_name = 'Michael Jordan';

-- this kind is useful to reduce the shuffling and optimize the query effeciency
-- to see the season_stats_r for individual years we can use UNNEST
SELECT player_name, 
	UNNEST(season_stats_r) as season_stats
FROM players_r
WHERE current_season = 2001 AND player_name = 'Michael Jordan';


-- we bringback the season_stats_r array with their column names
with unnested AS (
	SELECT player_name,
		UNNEST(season_stats_r)::season_stats_r AS season_stats
	FROM players_r
	WHERE current_season = 2001 
	AND player_name = 'Michael Jordan'
)
SELECT player_name,
	(season_stats::season_stats_r).*
FROM unnested;

--- this unnests season_stats_r array along with their columns and all the data are see as sorted form
-- which helps in run length encoding compression
-- by keeping all the temporal component seasons_stats in sorting order 
with unnested AS (
	SELECT player_name,
		UNNEST(season_stats_r)::season_stats_r AS season_stats
	FROM players_r
	WHERE current_season = 2001 
)
SELECT player_name,
	(season_stats::season_stats_r).*
FROM unnested;



-- analytics player performance from their 1st season to their latest season.
-- we get thier 1st & latest season details from below query.
SELECT 
	player_name,
	season_stats_r[1] AS first_season,
	season_stats_r[CARDINALITY(season_stats_r)] AS latest_season
FROM players_r
WHERE current_season = 2001

-- now get the points they scored
SELECT 
	player_name,
	(season_stats_r[1]:: season_stats_r).pts AS first_season,
	(season_stats_r[CARDINALITY(season_stats_r)]:: season_stats_r).pts AS latest_season
FROM players_r
WHERE current_season = 2001

-- make the math & get the performance improvement percentage without using groupby &
-- then aggregating the temporal attribute which shuffles data we used array and it becomes insanely fast
-- and infinetly parallelizable 
SELECT 
	player_name,
	(season_stats_r[CARDINALITY(season_stats_r)]:: season_stats_r).pts/
	CASE WHEN (season_stats_r[1]:: season_stats_r).pts = 0 THEN 1 
		ELSE (season_stats_r[1]:: season_stats_r).pts
	END AS performance_improvement
FROM players_r
WHERE current_season = 2001
AND scoring_class_r = 'star';