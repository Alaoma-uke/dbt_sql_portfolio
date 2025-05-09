{{ config
    (
      materialized = 'table',
      partition_by = 
        {
        "field": "country_name",
        "data_type": "string"
        },
     cluster_by = ["league_name", "season"],
     unique_key = "unique_id",
    ) 
}}

WITH games AS 
	(
		SELECT --mp.*,
            match_api_id,
			cc.name AS country_name, 
			lg.name AS league_name,
			season,
			date::DATE AS date,
			hteam.team_long_name AS home_team_name_l,
			hteam.team_short_name AS home_team_name_s,
			ateam.team_long_name AS away_team_name_l,
			ateam.team_short_name AS away_team_name_s,
			CASE WHEN COALESCE(home_team_goal,0) = COALESCE(away_team_goal,0) THEN 'draw'
				 WHEN COALESCE(home_team_goal,0) > COALESCE(away_team_goal,0) THEN 'home win'
				 WHEN COALESCE(home_team_goal,0) < COALESCE(away_team_goal,0) THEN 'away win'
			END AS match_result,
			home_team_goal,
			away_team_goal
			-- pp.player_name

		FROM {{source('staging','matches_played')}} mp
		JOIN {{source('staging','country')}} cc
			ON cc.id = mp.country_id 
		
        JOIN {{source('staging','league')}} lg
			USING (country_id)
		
        LEFT JOIN {{source('staging','team')}} hteam
			ON hteam.team_api_id = mp.away_team_api_id
		
        LEFT JOIN {{source('staging','team')}} ateam
			ON ateam.team_api_id = mp.home_team_api_id
		
        LEFT JOIN {{source('staging','player')}} pp 
			ON pp.player_api_id = mp.home_player_1
	),
	
final_cte AS 
    (
        SELECT {{dbt_utils.generate_surrogate_key(['match_api_id','country_name', 'date'])}} AS unique_id,
        *
	    FROM games
    )

SELECT *
FROM final_cte