{{ config(
    materialized='streaming',
    topic='random_users',
    checkpoint='s3a://raw-from-kafka/_checkpoints/random_users',
    target_table='default.random_users'
) }}

select * from stream_random_users