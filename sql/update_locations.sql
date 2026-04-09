CREATE OR REPLACE PROCEDURE mnl_accident_pipeline_dataset.upsert_locations()
BEGIN

  BEGIN TRANSACTION;

  -- Insert only new records
  INSERT INTO mnl_accident_pipeline_dataset.locations (locationid, city, location, latitude, longitude, high_accuracy)
  SELECT
    s.locationid,
    s.city,
    s.location,
    s.latitude,
    s.longitude,
    s.high_accuracy
  FROM (
    SELECT
      TO_HEX(SHA256(CONCAT(
        IFNULL(city, ''),
        IFNULL(location, '')
      ))) AS locationid,
      city,
      location,
      latitude,
      longitude,
      high_accuracy
    FROM mnl_accident_pipeline_dataset.locations_staging
  ) s
  LEFT JOIN mnl_accident_pipeline_dataset.locations t
    ON s.locationid = t.locationid
  WHERE t.locationid IS NULL;

  -- Optional: clear staging after success
  TRUNCATE TABLE mnl_accident_pipeline_dataset.locations_staging;

  COMMIT TRANSACTION;

END;