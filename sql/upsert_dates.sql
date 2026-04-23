CREATE OR REPLACE PROCEDURE mnl_accident_pipeline_dataset.upsert_dates()
BEGIN

  BEGIN TRANSACTION;

  INSERT INTO mnl_accident_pipeline_dataset.dim_dates (date, year, month, day, week, weekday)

  WITH staging AS (
    SELECT DISTINCT
      date, 
      year, 
      month, 
      day, 
      week, 
      weekday
    FROM mnl_accident_pipeline_dataset.staging_enriched
)

  SELECT
    s.date, 
    s.year, 
    s.month, 
    s.day, 
    s.week, 
    s.weekday
  FROM staging s
  LEFT JOIN mnl_accident_pipeline_dataset.dim_dates d
    ON s.date = d.date
  WHERE d.date IS NULL;

COMMIT TRANSACTION;

END;