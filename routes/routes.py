from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from logic import CustomerDataManager,SupplierDataManger
from sqlmodel import create_engine



router = APIRouter()
engine = create_engine("sqlite:///storage/invoice_tracker_null.db")

class customerOfferRequest(BaseModel):
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

@router.post("/calculate-offers-customer")
async def calculate_offers_customer(request: customerOfferRequest):
    try:
        
        input_data = request.dict()

       
        input_data.update({
            14: input_data.pop("d_14"),
            21: input_data.pop("d_21"),
            30: input_data.pop("d_30"),
            45: input_data.pop("d_45"),
            60: input_data.pop("d_60"),
        })
        fetcher=CustomerDataManager(engine)
        result = fetcher.offer_processor(input_data)

        
        return {"message": result}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Offer calculation failed: {str(e)}")

class SuppplierOfferRequest(BaseModel):

    d_14: tuple
    d_15: tuple
    d_20 :tuple
    d_30: tuple
    d_45: tuple
    d_60 :tuple
    d_90: tuple
    d_365:tuple

    
@router.post('/calculate-offer-supplier')
async def calculate_offer_supplier(request: SuppplierOfferRequest):
    try:
        input_data = request.dict()

       
        formatted_data = {
            int(key.split('_')[1]): value
            for key, value in input_data.items()
        }

       
        fetcher = SupplierDataManger(engine)
        result = fetcher.offer_processor(formatted_data)

   
        
        return {'message':result}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Supplier offer calculation failed: {str(e)}")