from pydantic import BaseModel
from typing import Optional, List, Union

def safe_decode(value):
    if isinstance(value, bytes): return value.decode('utf-8')
    return value
class PersonData(BaseModel): name: str
class VehicleData(BaseModel): 
    name: str
    radio_name: Optional[str] = ""
    status: Optional[int] = 2
    tuv_date: Optional[str] = None
    sp_date: Optional[str] = None
    milage: Optional[int] = 0
    next_service: Optional[str] = None

class EntryDto(BaseModel): 
    person_id: int; is_present: bool; note: Optional[str] = ""; 
    vehicle: Optional[str] = ""; signature: Optional[str] = None
class AttendanceUpload(BaseModel): 
    session_id: Optional[Union[int, str]] = None; date: str; time: Optional[str] = ""; end_time: Optional[str] = ""; group_id: int; category: str = "Übung"; 
    duration: float = 0.0; description: str; instructors: Optional[str] = ""; 
    leader_signature: Optional[str] = None; entries: List[EntryDto]
class GroupData(BaseModel): name: str

