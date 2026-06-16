from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import health, auth, datasets, analyses, tasks
from app.config import Config
from app.services.infra import init_sentry, limiter, LogRequestMiddleware

from slowapi.errors import RateLimitExceeded
from slowapi import _rate_limit_exceeded_handler


app = FastAPI()

app.include_router(health.router)
app.include_router(auth.router)
app.include_router(datasets.router)
app.include_router(analyses.router)
app.include_router(tasks.router)

init_sentry()

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(LogRequestMiddleware)

origins = [
    "http://localhost:8501",
    "http://127.0.0.1:8501",
    Config.PUBLIC_URL,
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)
