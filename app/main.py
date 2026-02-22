from fastapi import FastAPI
from app.core.config import settings
from app.routers import users

app = FastAPI(title=settings.PROJECT_NAME)

app.include_router(users.router)

@app.get("/")
def read_root():
    return {"status": "Online", "project": settings.PROJECT_NAME}
