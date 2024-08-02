WITH avg_stats AS (
    SELECT 
        game_id, 
        AVG(fgm) AS avg_fgm, 
        AVG(fga) AS avg_fga, 
        AVG(fg3m) AS avg_fg3m, 
        AVG(fg3a) AS avg_fg3a, 
        AVG(ftm) AS avg_ftm, 
        AVG(fta) AS avg_fta, 
        AVG(reb) AS avg_reb, 
        AVG(ast) AS avg_ast, 
        AVG(stl) AS avg_stl, 
        AVG(blk) AS avg_blk, 
        AVG(turnover) AS avg_turnover, 
        AVG(pf) AS avg_pf, 
        AVG(pts) AS avg_pts
    FROM player_game
    GROUP BY game_id
)

SELECT 
    game.season, 
    AVG(game.home_team_score) AS avg_home_team_score, 
    AVG(game.visitor_team_score) AS avg_visitor_team_score, 
    AVG(avg_stats.avg_fgm) AS avg_fgm,
    AVG(avg_stats.avg_fga) AS avg_fga,
    AVG(avg_stats.avg_fg3m) AS avg_fg3m,
    AVG(avg_stats.avg_fg3a) AS avg_fg3a,
    AVG(avg_stats.avg_ftm) AS avg_ftm,
    AVG(avg_stats.avg_fta) AS avg_fta,
    AVG(avg_stats.avg_reb) AS avg_reb,
    AVG(avg_stats.avg_ast) AS avg_ast,
    AVG(avg_stats.avg_stl) AS avg_stl,
    AVG(avg_stats.avg_blk) AS avg_blk,
    AVG(avg_stats.avg_turnover) AS avg_turnover,
    AVG(avg_stats.avg_pf) AS avg_pf,
    AVG(avg_stats.avg_pts) AS avg_pts
FROM game
INNER JOIN avg_stats ON game.game_id = avg_stats.game_id
GROUP BY season
ORDER BY season;