from modules.discord import pointers
from modules import accounts

from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi import FastAPI, Request

api = FastAPI()
api.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@api.get("/")
def get_home() -> JSONResponse:
    return JSONResponse({"status": "ok"})


# login with discord id + password
# get user instances
# fs utils

