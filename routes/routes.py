from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Dict, Any, Optional
import pandas as pd
from logic import offer_processor  # adjust the import as per your structure

router = APIRouter()

class OfferRequest(BaseModel):
    exchange_rate: float
    cost_of_finance_per_day: float
    max_discount_amount: float
    persentage_of_discount_savings: float
    minimum_threshold: float
    d_14: int
    d_21: int
    d_30: int
    d_45: int
    d_60 :int 

@router.post("/calculate-offers")
async def calculate_offers(request: OfferRequest):
    try:
        
        input_data = request.dict()

       
        input_data.update({
            14: input_data.pop("d_14"),
            21: input_data.pop("d_21"),
            30: input_data.pop("d_30"),
            45: input_data.pop("d_45"),
            60: input_data.pop("d_60"),
        })

        result_df = offer_processor(input_data)

        if isinstance(result_df, pd.DataFrame):
            return result_df.to_dict(orient="records")
        else:
            return {"message": result_df}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Offer calculation failed: {str(e)}")
