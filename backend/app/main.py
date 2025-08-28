from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.v1.routes import router as api_router

app = FastAPI(title="FHA Air Quality API")

origins = ["http://localhost:3000","http://localhost:8081","*"]  # relax for dev
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins, allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

app.include_router(api_router, prefix="/api/v1")

@app.get("/health")
def health():
    return {"ok": True}
