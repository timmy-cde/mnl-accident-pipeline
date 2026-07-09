CREATE OR REPLACE PROCEDURE mnl_accident_pipeline_dataset.upsert_vehicles()
BEGIN

  BEGIN TRANSACTION;

  -- INSERT IN dim_vehicles
  INSERT INTO mnl_accident_pipeline_dataset.dim_vehicles (vehicle_type, vehicle_group, suggested_from, is_verified)
  SELECT
    s.vehicle_type,
    s.vehicle_group,
    s.suggested_from,
    s.is_verified
  FROM mnl_accident_pipeline_dataset.staging_vehicles s
  LEFT JOIN mnl_accident_pipeline_dataset.dim_vehicles t
    ON s.vehicle_type = t.vehicle_type
  WHERE s.vehicle_type IS NOT NULL
    AND s.vehicle_group IS NOT NULL
    AND t.vehicle_type IS NULL;

  -- INSERT IN fact_event_vehicles 
  INSERT INTO mnl_accident_pipeline_dataset.fact_event_vehicles (fev_id, event_id, vehicle_type, vehicle_count)
  SELECT
    TO_HEX(SHA256(CONCAT(
      CAST(s.event_id AS STRING), '|', CAST(s.vehicle_type AS STRING)
    ))) AS fev_id,
    s.event_id,
    s.vehicle_type,
    s.vehicle_count
  FROM mnl_accident_pipeline_dataset.staging_vehicles s
  LEFT JOIN mnl_accident_pipeline_dataset.fact_event_vehicles t
    ON s.event_id = t.event_id AND s.vehicle_type = t.vehicle_type
  WHERE s.event_id IS NOT NULL
    AND s.vehicle_type IS NOT NULL
    AND t.event_id IS NULL
    AND t.vehicle_type IS NULL;

  -- TRUNCATE staging table
  TRUNCATE TABLE mnl_accident_pipeline_dataset.staging_vehicles;

  COMMIT TRANSACTION;

END;