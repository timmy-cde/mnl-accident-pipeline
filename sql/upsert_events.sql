CREATE OR REPLACE PROCEDURE mnl_accident_pipeline_dataset.upsert_events()
BEGIN

  BEGIN TRANSACTION;

  INSERT INTO mnl_accident_pipeline_dataset.fact_events (id, event_timestamp, location_id, event_type, direction_id, lanes_blocked, involved, post, link)

  WITH staging AS (
        SELECT
            TO_HEX(SHA256(CONCAT(
                IFNULL(SAFE_CAST(date AS STRING), ''),
                IFNULL(SAFE_CAST(time AS STRING), ''),
                IFNULL(SAFE_CAST(city AS STRING), ''),
                IFNULL(SAFE_CAST(location AS STRING), ''),
                IFNULL(SAFE_CAST(latitude AS STRING), ''),
                IFNULL(SAFE_CAST(longitude AS STRING), ''),
                IFNULL(SAFE_CAST(post AS STRING), '')
            ))) AS id,
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
            direction,
            lanes_blocked,
            involved,
            post,
            link
        FROM mnl_accident_pipeline_dataset.staging_enriched
    ), 
    agg AS (
        SELECT
            s.id,
            s.event_timestamp,
            s.location_id,
            s.event_type,
            IFNULL(dir.short_name, 'UNKNOWN') AS direction_id,
            s.lanes_blocked,
            s.involved,
            s.post,
            s.link
        FROM staging s

        LEFT JOIN mnl_accident_pipeline_dataset.dim_direction dir
            ON s.direction IN (dir.short_name, dir.full_name)
    )

    SELECT
        a.id,
        a.event_timestamp,
        a.location_id,
        a.event_type,
        a.direction_id,
        a.lanes_blocked,
        a.involved,
        a.post,
        a.link
    FROM agg a
    LEFT JOIN mnl_accident_pipeline_dataset.fact_events f
        ON a.id = f.id
    WHERE f.id IS NULL;

-- Optional: clear staging after success
TRUNCATE TABLE mnl_accident_pipeline_dataset.staging_enriched;

COMMIT TRANSACTION;

END;