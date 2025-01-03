import os
import json
import requests
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes
from pyflink.table.udf import ScalarFunction, udf



def create_processed_events_source_kafka(t_env):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "process_events_kafka"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    
    # Create Kafka source table
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_time VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR,
            window_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_aggregated_events_sink_postgres(t_env):
    table_name = 'processed_events_sessioned'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            geodata VARCHAR,
            host VARCHAR,
            session_time TIMESTAMP(3),
            num_hits BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


class GetLocation(ScalarFunction):
  def eval(self, ip_address):
    print("Getting location for IP: " + ip_address)
    url = "https://api.ip2location.io"
    response = requests.get(url, params={
        'ip': ip_address,
        'key': os.environ.get("IP_CODING_KEY")
    })

    if response.status_code != 200:
        # Return empty dict if request failed
        return json.dumps({})

    data = json.loads(response.text)

    # Extract the country and state from the response
    # This might change depending on the actual response structure
    country = data.get('country_code', '')
    state = data.get('region_name', '')
    city = data.get('city_name', '')
    return json.dumps({'country': country, 'state': state, 'city': city})

get_location = udf(GetLocation(), result_type=DataTypes.STRING())


def process_events(t_env, source_table, target_table):
    # Use Flink SQL to sessionize events based on IP and host with a 5-minute gap
    query = f"""
        INSERT INTO {target_table}
        SELECT
            ip,
            get_location(ip) AS geodata,  -- UDF call for geolocation
            host,
            session_start,
            num_hits
        FROM 
        (
            SELECT
                ip,
                host,
                SESSION_START(window_timestamp, INTERVAL '5' MINUTE) AS session_start,
                COUNT(*) AS num_hits
            FROM {source_table}
            GROUP BY ip, host, SESSION(window_timestamp, INTERVAL '5' MINUTE)
        );
    """
    
    # Execute the query to process the events and insert into the PostgreSQL sink
    t_env.execute_sql(query).wait()


def main():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(60 * 1000)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    t_env.create_temporary_function("get_location", get_location)

    # Create source (Kafka) and sink (PostgreSQL) tables
    source_table = create_processed_events_source_kafka(t_env)
    target_table = create_aggregated_events_sink_postgres(t_env)

    # Process the events by applying the session window aggregation
    process_events(t_env, source_table, target_table)


if __name__ == '__main__':
    main()
