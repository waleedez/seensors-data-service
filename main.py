from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

load_dotenv()

app = FastAPI()

# Database connection settings
DATABASE_URL = os.getenv("POSTGRES_URL")

# Create SQLAlchemy engine and session
engine = create_async_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, class_=AsyncSession)
Base = declarative_base()


@app.get("/")
async def healthcheck():
    return {"status": "ok"}


@app.get("/sensors_ids")
async def get_sensors_ids():
    async with engine.connect() as connection:
        try:
            # Perform a query to fetch all users
            result = await connection.execute(text("SELECT distinct id FROM sensors_data"))
            sensors_data = result.fetchall()
            
            # Convert query result to list of dicts
            data_list = [data[0] for data in sensors_data]
            return data_list

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/sensors_data")
async def get_sensors_data(sensors_ids: str, timespan: int):
    async with engine.connect() as connection:
        try:
            # Perform a query to fetch all users
            result = await connection.execute(text(f"SELECT distinct id, timestamp, reading FROM sensors_data where id in ({sensors_ids}) and timestamp > NOW() - INTERVAL '{timespan} minutes'"))
            sensors_data = result.fetchall()
            
            # Convert query result to list of dicts
            data_list = [{"id": data[0], "timestamp": data[1], "reading": data[2]} for data in sensors_data]
            return data_list

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
