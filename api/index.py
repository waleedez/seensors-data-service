from flask import Flask, jsonify, request
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Database connection settings
DATABASE_URL = os.getenv("POSTGRES_URL")

# Create SQLAlchemy engine and session
engine = create_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


@app.route("/")
def healthcheck():
    return jsonify({"status": "ok"})


@app.route("/sensors_ids", methods=["GET"])
def get_sensors_ids():
    with engine.connect() as connection:
        try:
            # Perform a query to fetch all distinct sensor IDs
            result = connection.execute(text("SELECT distinct id FROM sensors_data"))
            sensors_data = result.fetchall()
            
            # Convert query result to list of ids
            data_list = [data[0] for data in sensors_data]
            return jsonify(data_list)

        except Exception as e:
            return jsonify({"error": str(e)}), 500


@app.route("/sensors_data", methods=["GET"])
def get_sensors_data():
    sensors_ids = request.args.get("sensors_ids")
    timespan = request.args.get("timespan")

    with engine.connect() as connection:
        try:
            # Perform a query to fetch sensor data based on sensors_ids and timespan
            query = text(f"""
                SELECT distinct id, timestamp, reading 
                FROM sensors_data 
                WHERE id in ({sensors_ids}) 
                AND timestamp > NOW() - INTERVAL '{timespan} minutes'
            """)
            result = connection.execute(query)
            sensors_data = result.fetchall()
            
            # Convert query result to list of dicts
            data_list = [{"id": data[0], "timestamp": data[1], "reading": data[2]} for data in sensors_data]
            return jsonify(data_list)

        except Exception as e:
            return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
