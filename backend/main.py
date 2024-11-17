from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import io
from process_data import calculate_metrics_async
from db import create_tables

app = FastAPI()


@app.post("/upload")
async def upload_files(
    account: UploadFile = File(...), portfolio: UploadFile = File(...)
):
    # Read files into dataframes
    account_df = pd.read_csv(io.BytesIO(await account.read()))
    portfolio_df = pd.read_csv(io.BytesIO(await portfolio.read()))

    # Start db
    create_tables("stocks.db")

    # Call the calculation function
    metrics = await calculate_metrics_async(account_df, portfolio_df)

    return JSONResponse(content=metrics)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # React app's URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
