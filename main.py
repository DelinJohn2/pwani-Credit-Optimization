from fastapi import FastAPI
from routes.routes import router as offer_router  # Adjust the import if needed

app = FastAPI()

app.include_router(offer_router, prefix="/offers", tags=["Offer Calculator"])

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)