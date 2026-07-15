CREATE OR REPLACE PROCEDURE mnl_accident_pipeline_dataset.upsert_events()
BEGIN

  BEGIN TRANSACTION;

  INSERT INTO mnl_accident_pipeline_dataset.fact_events (event_id, event_timestamp, location_id, event_type, event_type_details, direction_id, lanes_blocked, post, link)

  WITH staging AS (
        SELECT
            event_id,
            event_timestamp,
            location_id,
            CASE
                WHEN type LIKE 'STALLED%' THEN 'STALLED VEHICLE'
                WHEN type LIKE 'ROAD%' THEN 'ROAD CRASH'
                WHEN type LIKE 'VEHICULAR%' THEN 'ACCIDENT'
                WHEN type LIKE '%COLLISION%' THEN 'MULTIPLE COLLISION'
                WHEN type LIKE 'RALLY%' THEN 'RALLY'
                ELSE 'OTHERS'
            END AS event_type,
            type as event_type_details,
            direction,
            lanes_blocked,
            post,
            link
        FROM mnl_accident_pipeline_dataset.staging_events
    ), 
    agg AS (
        SELECT
            s.event_id,
            s.event_timestamp,
            s.location_id,
            s.event_type,
            s.event_type_details,
            IFNULL(dir.short_name, 'UNKNOWN') AS direction_id,
            s.lanes_blocked,
            s.post,
            s.link
        FROM staging s

        LEFT JOIN mnl_accident_pipeline_dataset.dim_direction dir
            ON s.direction IN (dir.short_name, dir.full_name)
    )

    SELECT
        a.event_id,
        a.event_timestamp,
        a.location_id,
        a.event_type,
        a.event_type_details,
        a.direction_id,
        a.lanes_blocked,
        a.post,
        a.link
    FROM agg a
    LEFT JOIN mnl_accident_pipeline_dataset.fact_events f
        ON a.event_id = f.event_id
    WHERE f.event_id IS NULL;

-- Optional: clear staging after success
TRUNCATE TABLE mnl_accident_pipeline_dataset.staging_events;

COMMIT TRANSACTION;

END;