BEGIN TRANSACTION;

INSERT INTO mnl_accident_pipeline_dataset.dim_direction (short_name, full_name)
VALUES
  ('NB', 'North Bound'),
  ('SB', 'South Bound'),
  ('EB', 'East Bound'),
  ('WB', 'West Bound');

COMMIT TRANSACTION;