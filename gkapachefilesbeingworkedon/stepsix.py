import json
from kafka import KafkaConsumer
from superset import db, security_manager
from superset.models.core import Database, Slice, Dash
from superset.connectors.sqla.models import SqlMetric, TableColumn

# Initialize Kafka Consumer
consumer = KafkaConsumer('aggregated_scores_topic',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                         auto_offset_reset='latest',
                         enable_auto_commit=True,
                         group_id='visualization_consumer_group')

# Create a SQLite database for storing the sentiment data
database = Database(
    database_name="sentiment_data",
    sqlalchemy_uri="sqlite:///sentiment_data.db",
    cache_timeout=3600,
    allow_csv_upload=True,
)
db.session.add(database)
db.session.commit()

# Create a table for the sentiment data
table = SqlaTable(table_name="sentiment_scores")
table.database = database
table.columns = [
    TableColumn(column_name="ticker", type="STRING"),
    TableColumn(column_name="aggregated_score", type="FLOAT"),
]
table.metrics = [SqlMetric(metric_name="count", expression="count(*)")]
db.session.add(table)
db.session.commit()

# Function to visualize and report sentiment analysis results
def visualize_and_report():
    for message in consumer:
        sentiment_data = message.value
        ticker = sentiment_data['ticker']
        aggregated_score = sentiment_data['aggregated_score']

        # Insert the data into the SQLite database
        table.get_sqla_table().insert().values(
            ticker=ticker,
            aggregated_score=aggregated_score
        ).execute()

        # Create a Slice (chart) in Superset
        slc = Slice(
            slice_name=f"{ticker} Sentiment Score",
            viz_type="big_number",
            datasource_type="table",
            datasource_id=table.id,
            params=json.dumps({
                "metric": "count",
                "colNames": ["aggregated_score"],
                "comparisons": [],
                "headerFontSize": 16,
                "footerFontSize": 14,
            }),
        )
        db.session.add(slc)
        db.session.commit()

        # Create a Dashboard in Superset
        dash = Dash(dashboard_title=f"{ticker} Sentiment Dashboard")
        dash.slices.append(slc)
        db.session.add(dash)
        db.session.commit()

        print(f"Visualization and reporting for {ticker}: {aggregated_score}")

# Start the visualization and reporting process
visualize_and_report()