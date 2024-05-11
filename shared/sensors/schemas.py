from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class Sensor(BaseModel):
    id: int
    name: str
    latitude: float
    longitude: float
    # new: canvi de comments
    # joined_at: str | None = None
    joined_at: datetime | None = None
    # last_seen: str | None = None
    last_seen: datetime | None = None
    type: str 
    mac_address: str
    manufacturer: str #.
    model: str #.
    serie_number: str #.
    firmware_version: str #.
    battery_level: float | None = None
    temperature: float | None = None 
    humidity: float | None = None
    velocity:  float | None = None
    description: str
    
    
    class Config:
        orm_mode = True
        
class SensorCreate(BaseModel):
    name: str
    longitude: float
    latitude: float
    # new: last_seen i joined_at no estaven
    # joined_at: Optional[datetime]= None #.
    # last_seen: Optional[datetime]= None #.
    type: str
    mac_address: str
    manufacturer: str
    model: str
    serie_number: str
    firmware_version: str
    description: str #.

class SensorData(BaseModel):
    velocity: Optional[float]= None
    temperature: Optional[float]= None
    humidity: Optional[float]= None
    battery_level: Optional[float]= None
    # new: canvi de comments
    # last_seen: Optional[str]= None
    last_seen: str
   