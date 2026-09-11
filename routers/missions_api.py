from fastapi import APIRouter, HTTPException, Request, Response, UploadFile, File
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Optional, List, Union
from datetime import datetime, date
import json

from database import get_db_connection
from core.utils import get_current_user, log_audit_action

router = APIRouter()

