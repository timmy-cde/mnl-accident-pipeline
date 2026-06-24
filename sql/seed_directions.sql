BEGIN TRANSACTION;

INSERT INTO `mnl_accident_pipeline_dataset.dim_direction` (short_name, full_name)
VALUES
  ('NB', 'NORTHBOUND'),
  ('SB', 'SOUTHBOUND'),
  ('EB', 'EASTBOUND'),
  ('WB', 'WESTBOUND');

COMMIT TRANSACTION;