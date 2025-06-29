from fastapi import FastAPI, Depends, Query
from sqlalchemy.orm import Session
from typing import List

from datamart.api.utils.db import get_db, SessionLocal
from datamart.api.schemas.responses import AccidentMetrics
from datamart.api.routes import geo, time

app = FastAPI(
    title="US Accidents Datamart API",
    description="API permettant d'interroger les métriques sur les accidents (par ville, par jour, etc.)",
    version="1.0.0",
)

# Routes personnalisées
app.include_router(geo.router, prefix="/geo", tags=["Géographie"])
app.include_router(time.router, prefix="/time", tags=["Temps"])

@app.get("/", tags=["Root"])
def read_root():
    return {"message": "Bienvenue sur l’API Datamart des US Accidents"}
