from fastapi import FastAPI
from routes import routes # import your router module

app = FastAPI(
    title="Pwani Finance Offer Calculator",

)

# Include router
app.include_router(routes.router, prefix="/api", tags=["Offer Routes"])

# Root route
@app.get("/")
async def root():
    return {"message": "Pwani Finance Offer API is running"}