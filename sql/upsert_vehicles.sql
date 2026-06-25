CREATE OR REPLACE PROCEDURE mnl_accident_pipeline_dataset.upsert_vehicles()
BEGIN

  BEGIN TRANSACTION;

  -- INSERT IN dim_vehicles

  -- INSERT IN fact_event_vehicles 

  COMMIT TRANSACTION;

END;