/* P2.1 candidate indexes - apply only after diagnostics review.
   All statements are idempotent.
*/
SET NOCOUNT ON;

/* Candidate 1: map app lookup used in VW_TCO_FEATURE_DEMAND mapping join */
IF COL_LENGTH('dbo.MAP_ADO_APP_TO_TCO_GROUP', 'ADO_APP') IS NOT NULL
   AND COL_LENGTH('dbo.MAP_ADO_APP_TO_TCO_GROUP', 'APP_GROUP') IS NOT NULL
   AND NOT EXISTS (
     SELECT 1
     FROM sys.indexes
     WHERE object_id = OBJECT_ID('dbo.MAP_ADO_APP_TO_TCO_GROUP')
       AND name = 'IX_MAP_ADO_APP_TO_GROUP_ADO_APP'
   )
BEGIN
  CREATE INDEX IX_MAP_ADO_APP_TO_GROUP_ADO_APP
    ON dbo.MAP_ADO_APP_TO_TCO_GROUP (ADO_APP)
    INCLUDE (APP_GROUP);
END;

/* Candidate 2: team->program navigation for scoped joins/filtering */
IF COL_LENGTH('dbo.TEAMS', 'PROGRAMID') IS NOT NULL
   AND COL_LENGTH('dbo.TEAMS', 'TEAMID') IS NOT NULL
   AND COL_LENGTH('dbo.TEAMS', 'TEAMNAME') IS NOT NULL
   AND NOT EXISTS (
     SELECT 1
     FROM sys.indexes
     WHERE object_id = OBJECT_ID('dbo.TEAMS')
       AND name = 'IX_TEAMS_PROGRAMID_TEAMID'
   )
BEGIN
  CREATE INDEX IX_TEAMS_PROGRAMID_TEAMID
    ON dbo.TEAMS (PROGRAMID, TEAMID)
    INCLUDE (TEAMNAME);
END;

/* Candidate 3: optional ADO features composite index for year+team variant paths.
   Enable only when diagnostics show high reads on ADO_FEATURES in demand path.
*/
/*
IF COL_LENGTH('dbo.ADO_FEATURES', 'ADO_YEAR') IS NOT NULL
   AND COL_LENGTH('dbo.ADO_FEATURES', 'TEAM_VARIANT_KEY') IS NOT NULL
   AND COL_LENGTH('dbo.ADO_FEATURES', 'ITERATION_SK') IS NOT NULL
   AND COL_LENGTH('dbo.ADO_FEATURES', 'FEATURE_ID') IS NOT NULL
   AND COL_LENGTH('dbo.ADO_FEATURES', 'STORY_POINTS') IS NOT NULL
   AND COL_LENGTH('dbo.ADO_FEATURES', 'STATE') IS NOT NULL
   AND NOT EXISTS (
     SELECT 1
     FROM sys.indexes
     WHERE object_id = OBJECT_ID('dbo.ADO_FEATURES')
       AND name = 'IX_ADO_FEATURES_YEAR_TEAMVAR_ITER'
   )
BEGIN
  CREATE INDEX IX_ADO_FEATURES_YEAR_TEAMVAR_ITER
    ON dbo.ADO_FEATURES (ADO_YEAR, TEAM_VARIANT_KEY, ITERATION_SK)
    INCLUDE (FEATURE_ID, STORY_POINTS, STATE, TEAM_RAW, APP_NAME_RAW, ITERATION_LEVEL3);
END;
*/
