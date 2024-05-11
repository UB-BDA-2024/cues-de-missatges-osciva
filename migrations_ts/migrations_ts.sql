-- #TODO: Create new TS hypertable



CREATE TABLE IF NOT EXISTS sensor_data ( 
    sensor_id integer NOT NULL,     
    temperature float,
    velocity FLOAT,
    humidity float,
    battery_level float, 
    last_seen timestamptz NOT NULL,  
    PRIMARY KEY (sensor_id, last_seen)
    );

SELECT create_hypertable('sensor_data', 'last_seen');
