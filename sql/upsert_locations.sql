CREATE OR REPLACE PROCEDURE mnl_accident_pipeline_dataset.upsert_locations()
BEGIN

  BEGIN TRANSACTION;

  -- Insert only new records
  INSERT INTO mnl_accident_pipeline_dataset.dim_locations (location_id, city, location, latitude, longitude, accuracy)

  WITH staging AS (
    SELECT
      TO_HEX(SHA256(CONCAT(
        IFNULL(city, ''),
        IFNULL(location, '')
      ))) AS location_id,
      city,
      location,
      latitude,
      longitude,
      accuracy
    FROM mnl_accident_pipeline_dataset.staging_locations
    WHERE city IS NOT NULL
      AND location IS NOT NULL
      AND latitude IS NOT NULL
      AND longitude IS NOT NULL
  )

  SELECT
    s.location_id,
    s.city,
    s.location,
    s.latitude,
    s.longitude,
    s.accuracy
  FROM staging s
  LEFT JOIN mnl_accident_pipeline_dataset.dim_locations t
    ON s.location_id = t.location_id
  WHERE t.location_id IS NULL;

  -- Optional: clear staging after success
  TRUNCATE TABLE mnl_accident_pipeline_dataset.staging_locations;

  COMMIT TRANSACTION;

END;