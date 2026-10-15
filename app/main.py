"""Employee Attendance & Analytics API. Run: uvicorn app.main:app --port 8000"""
import json
import os, re, calendar
from contextlib import asynccontextmanager
from datetime import datetime, date, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Any
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, ConfigDict, field_validator
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError
from bson import json_util

load_dotenv()
client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"), serverSelectionTimeoutMS=5000, tz_aware=True)
db = client[os.getenv("MONGO_DB", "attendance_db")]
IST = timezone(timedelta(hours=5, minutes=30)); UTC = timezone.utc
PRESENCE = ["PRESENT", "WFH", "ON_DUTY"]
STATUSES = PRESENCE + ["ABSENT", "LEAVE"]
EPOCH_MIN, EPOCH_MAX = 100_000_000_000, 4_102_444_800_000
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MONTH_RE = r"^\d{4}-(0[1-9]|1[0-2])$"
TIME_RE = r"^(?:[01]\d|2[0-3]):[0-5]\d$"


def now_utc(): return datetime.now(UTC).replace(microsecond=0)
def trunc_dt(dt):
    if dt.tzinfo is None: dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).replace(microsecond=0)
def epoch_ms(dt): return None if dt is None else int(trunc_dt(dt).timestamp() * 1000)
def from_ms(v):
    if v is None: return None
    if isinstance(v, bool) or not isinstance(v, int) or not EPOCH_MIN <= v <= EPOCH_MAX: raise HTTPException(422, "timestamp must be epoch milliseconds in the supported range")
    return datetime.fromtimestamp(v / 1000, UTC).replace(microsecond=0)
def parse_date(s, field="date"):
    if not isinstance(s, str) or not DATE_RE.fullmatch(s): raise HTTPException(422, f"{field} must use YYYY-MM-DD")
    try: d = date.fromisoformat(s)
    except ValueError: raise HTTPException(422, f"{field} must be a valid date")
    if d.isoformat() != s: raise HTTPException(422, f"{field} must use YYYY-MM-DD")
    return d
def month_bounds(m):
    if not re.fullmatch(MONTH_RE, m): raise HTTPException(422, "month must use YYYY-MM")
    y, mo = map(int, m.split("-")); return date(y, mo, 1), date(y, mo, calendar.monthrange(y, mo)[1])
def rh(v, n=2): return float(Decimal(str(v)).quantize(Decimal("1").scaleb(-n), rounding=ROUND_HALF_UP))
def ist_instant(day, clock):
    h, m = map(int, clock.split(":")); return datetime.combine(day, time(h, m), IST).astimezone(UTC).replace(microsecond=0)
def shift_bounds(emp, daystr):
    d = parse_date(daystr); start = ist_instant(d, emp["shift_start"])
    endday = d + timedelta(days=1) if emp["shift_end"] <= emp["shift_start"] else d
    return start, ist_instant(endday, emp["shift_end"])
def attendance_date(pin, emp):
    local = trunc_dt(pin).astimezone(IST); d = local.date()
    if emp["shift_end"] <= emp["shift_start"] and local.strftime("%H:%M") < emp["shift_end"]: d -= timedelta(days=1)
    return d.isoformat()
def late_minutes(pin, emp, daystr):
    if pin is None: return 0
    start, _ = shift_bounds(emp, daystr); sec = (trunc_dt(pin)-start).total_seconds()
    return max(0, int(sec // 60)) if sec > 600 else 0
def work_hours(pin, pout):
    if pin is None or pout is None: return None
    return rh((trunc_dt(pout)-trunc_dt(pin)).total_seconds()/3600)
def overtime_minutes(pout, emp, daystr):
    if pout is None: return 0
    _, end = shift_bounds(emp, daystr); mins = int((trunc_dt(pout)-end).total_seconds() // 60)
    return mins if mins >= 30 else 0
def derive(doc, emp):
    if doc.get("status") not in PRESENCE:
        doc.update(punch_in=None, punch_out=None, work_hours=None, late_minutes=0, overtime_minutes=0, half_day=False)
    else:
        doc["late_minutes"] = late_minutes(doc.get("punch_in"), emp, doc["date"])
        doc["work_hours"] = work_hours(doc.get("punch_in"), doc.get("punch_out"))
        doc["overtime_minutes"] = overtime_minutes(doc.get("punch_out"), emp, doc["date"])
        doc["half_day"] = doc["work_hours"] is not None and doc["work_hours"] < 4.50
    return doc
def serialize(v):
    if isinstance(v, datetime): return epoch_ms(v)
    if isinstance(v, list): return [serialize(x) for x in v]
    if isinstance(v, dict): return {k: serialize(x) for k,x in v.items() if k != "_id"}
    return v
def attendance_out(doc):
    x = {k:v for k,v in doc.items() if k != "_id"}
    x.setdefault("punch_in", None); x.setdefault("punch_out", None); x.setdefault("work_hours", None)
    x.setdefault("late_minutes", 0); x.setdefault("overtime_minutes", 0); x.setdefault("half_day", False); x.setdefault("history", [])
    return serialize(x)
def employee(code):
    x = db.employees.find_one({"emp_code":code}, {"_id":0})
    if not x: raise HTTPException(404, "employee not found")
    return x

@asynccontextmanager
async def lifespan(app):
    db.employees.create_index("emp_code", unique=True, name="uq_employee_code")
    db.employees.create_index([("department",1),("emp_code",1)], name="ix_employee_department_code")
    db.employees.create_index([("joined_on",1),("department",1)], name="ix_employee_joined_department")
    db.attendance_logs.create_index([("emp_code",1),("date",1)], unique=True, name="uq_attendance_employee_date")
    db.attendance_logs.create_index([("date",-1),("emp_code",1)], name="ix_attendance_date_employee")
    db.attendance_logs.create_index([("status",1),("date",-1)], name="ix_attendance_status_date")
    db.attendance_logs.create_index([("emp_code",1),("punch_in",-1)], name="ix_attendance_employee_punchin")
    yield
app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)

class EmployeeIn(BaseModel):
    model_config = ConfigDict(extra="ignore")
    emp_code: str = Field(pattern=r"^EMP\d{4,6}$")
    name: str = Field(min_length=1,max_length=100)
    email: str = Field(min_length=1,max_length=120,pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1,max_length=50)
    shift_start: str = "09:30"
    shift_end: str = "18:30"
    joined_on: str
    @field_validator("shift_start","shift_end")
    @classmethod
    def shift_format(cls,v):
        if not re.fullmatch(TIME_RE,v): raise ValueError("must use HH:MM")
        return v
    @field_validator("joined_on")
    @classmethod
    def date_format(cls,v):
        date.fromisoformat(v)
        if not DATE_RE.fullmatch(v): raise ValueError("must use YYYY-MM-DD")
        return v

class PunchInIn(BaseModel):
    model_config = ConfigDict(extra="ignore")
    emp_code: str
    punched_at: Optional[int] = None
    status: str = "PRESENT"
    @field_validator("punched_at")
    @classmethod
    def valid_ms(cls,v):
        if v is not None and (isinstance(v,bool) or not isinstance(v,int) or not EPOCH_MIN <= v <= EPOCH_MAX): raise ValueError("must be epoch milliseconds")
        return v
    @field_validator("status")
    @classmethod
    def valid_status(cls,v):
        if v not in PRESENCE: raise ValueError("status must be PRESENT, WFH, or ON_DUTY")
        return v
class PunchOutIn(BaseModel):
    model_config = ConfigDict(extra="ignore")
    emp_code: str
    punched_at: Optional[int] = None
    @field_validator("punched_at")
    @classmethod
    def valid_ms(cls,v):
        if v is not None and (isinstance(v,bool) or not isinstance(v,int) or not EPOCH_MIN <= v <= EPOCH_MAX): raise ValueError("must be epoch milliseconds")
        return v
class Correction(BaseModel):
    model_config = ConfigDict(extra="ignore")
    reason: str = Field(min_length=5,max_length=200)
    regularized_by: str = Field(min_length=1,max_length=50)
    status: Optional[str] = None
    punch_in: Optional[int] = None
    punch_out: Optional[int] = None
    @field_validator("status")
    @classmethod
    def valid_status(cls,v):
        if v is not None and v not in STATUSES: raise ValueError("invalid status")
        return v
    @field_validator("punch_in","punch_out")
    @classmethod
    def valid_ms(cls,v):
        if v is not None and (isinstance(v,bool) or not isinstance(v,int) or not EPOCH_MIN <= v <= EPOCH_MAX): raise ValueError("must be epoch milliseconds")
        return v

@app.get("/health")
def health():
    try: db.command("ping"); return {"status":"ok"}
    except PyMongoError: raise HTTPException(503,"Database unavailable")

@app.post("/employees",status_code=201)
def create_employee(body: EmployeeIn):
    if body.shift_start == body.shift_end: raise HTTPException(422,"shift_start and shift_end must differ")
    doc=body.model_dump(); doc["created_at"]=now_utc()
    try: db.employees.insert_one(doc)
    except DuplicateKeyError: raise HTTPException(409,"emp_code already exists")
    doc.pop("_id",None); doc["created_at"]=epoch_ms(doc["created_at"]); return doc

@app.get("/employees")
def list_employees(department: Optional[str]=None,page:int=Query(1,ge=1),page_size:int=Query(20,ge=1,le=100)):
    q={}
    if department is not None:q["department"]=department
    total=db.employees.count_documents(q)
    items=list(db.employees.find(q,{"_id":0}).sort("emp_code",1).skip((page-1)*page_size).limit(page_size))
    for x in items:
        if isinstance(x.get("created_at"),datetime):x["created_at"]=epoch_ms(x["created_at"])
    return {"items":items,"total":total,"page":page,"page_size":page_size}

@app.post("/attendance/punch-in",status_code=201)
def punch_in(body:PunchInIn):
    emp=employee(body.emp_code); ts=trunc_dt(from_ms(body.punched_at) if body.punched_at is not None else now_utc())
    day=attendance_date(ts,emp); doc={"emp_code":body.emp_code,"date":day,"status":body.status,"punch_in":ts,"punch_out":None,"work_hours":None,"late_minutes":0,"overtime_minutes":0,"half_day":False,"history":[]}; derive(doc,emp)
    try: db.attendance_logs.insert_one(doc)
    except DuplicateKeyError: raise HTTPException(409,"already punched in for this date")
    return attendance_out(doc)

@app.post("/attendance/punch-out")
def punch_out(body:PunchOutIn):
    emp=employee(body.emp_code); ts=trunc_dt(from_ms(body.punched_at) if body.punched_at is not None else now_utc())
    rec=db.attendance_logs.find_one({"emp_code":body.emp_code,"punch_in":{"$ne":None,"$lte":ts}},sort=[("punch_in",-1)])
    if not rec: raise HTTPException(404,"no punch-in found")
    if rec.get("punch_out") is not None: raise HTTPException(409,"already punched out")
    pin=trunc_dt(rec["punch_in"])
    if ts<=pin or ts>pin+timedelta(hours=24): raise HTTPException(422,"punch-out must be after punch-in and within 24 hours")
    new=dict(rec);new["punch_out"]=ts;derive(new,emp)
    result=db.attendance_logs.update_one({"_id":rec["_id"],"punch_out":None},{"$set":{"punch_out":ts,"work_hours":new["work_hours"],"overtime_minutes":new["overtime_minutes"],"half_day":new["half_day"]}})
    if result.modified_count!=1:raise HTTPException(409,"already punched out")
    return attendance_out(db.attendance_logs.find_one({"_id":rec["_id"]}))

@app.get("/attendance")
def list_attendance(emp_code:Optional[str]=None,date_from:Optional[str]=None,date_to:Optional[str]=None,status:Optional[str]=None,page:int=Query(1,ge=1),page_size:int=Query(20,ge=1,le=100)):
    q={}
    if emp_code is not None:q["emp_code"]=emp_code
    if date_from:parse_date(date_from,"date_from")
    if date_to:parse_date(date_to,"date_to")
    if date_from and date_to and date_from>date_to:raise HTTPException(422,"date_from must be <= date_to")
    if date_from or date_to:
        q["date"]={}
        if date_from:q["date"]["$gte"]=date_from
        if date_to:q["date"]["$lte"]=date_to
    if status is not None:
        if status not in STATUSES:raise HTTPException(422,"invalid status")
        q["status"]=status
    total=db.attendance_logs.count_documents(q)
    cur=db.attendance_logs.find(q,{"_id":0}).sort([("date",-1),("emp_code",1)]).skip((page-1)*page_size).limit(page_size)
    return {"items":[attendance_out(x) for x in cur],"total":total,"page":page,"page_size":page_size}

@app.patch("/attendance/{emp_code}/{date_str}")
def correct_attendance(emp_code:str,date_str:str,body:Correction):
    parse_date(date_str); emp=employee(emp_code); cur=db.attendance_logs.find_one({"emp_code":emp_code,"date":date_str})
    if not cur:raise HTTPException(404,"attendance record not found")
    old=dict(cur)
    for k,v in [("punch_in",None),("punch_out",None),("work_hours",None),("late_minutes",0),("overtime_minutes",0),("half_day",False),("history",[])]:old.setdefault(k,v)
    new=dict(old); supplied=body.model_fields_set
    if "status" in supplied:new["status"]=body.status
    if "punch_in" in supplied:new["punch_in"]=from_ms(body.punch_in)
    if "punch_out" in supplied:new["punch_out"]=from_ms(body.punch_out)
    if new["status"] not in PRESENCE:
        if ("punch_in" in supplied and body.punch_in is not None) or ("punch_out" in supplied and body.punch_out is not None):raise HTTPException(422,"ABSENT/LEAVE cannot have punch times")
        new["punch_in"]=new["punch_out"]=None
    else:
        if new.get("punch_in") is None:raise HTTPException(422,"presence status requires punch_in")
        if attendance_date(new["punch_in"],emp)!=date_str:raise HTTPException(422,"punch_in must belong to attendance date")
        if new.get("punch_out") is not None and (trunc_dt(new["punch_out"])<=trunc_dt(new["punch_in"]) or trunc_dt(new["punch_out"])>trunc_dt(new["punch_in"])+timedelta(hours=24)):raise HTTPException(422,"invalid punch_out")
    derive(new,emp); changes={}
    for k in ("status","punch_in","punch_out","work_hours","late_minutes","overtime_minutes","half_day"):
        a,b=old.get(k),new.get(k)
        if isinstance(a,datetime):a=trunc_dt(a)
        if isinstance(b,datetime):b=trunc_dt(b)
        if a!=b:changes[k]={"from":a,"to":b}
    if not changes:raise HTTPException(422,"correction does not change the record")
    hist={"at":now_utc(),"by":body.regularized_by,"reason":body.reason,"changes":changes}
    filt={"_id":cur["_id"]}
    for k in ("status","punch_in","punch_out","work_hours","late_minutes","overtime_minutes","half_day"):filt[k]=cur.get(k)
    result=db.attendance_logs.update_one(filt,{"$set":{k:new.get(k) for k in ("status","punch_in","punch_out","work_hours","late_minutes","overtime_minutes","half_day")},"$push":{"history":hist}})
    if result.modified_count!=1:raise HTTPException(409,"record changed concurrently; retry")
    return attendance_out(db.attendance_logs.find_one({"_id":cur["_id"]}))

@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code:str,month:str=Query(...,pattern=MONTH_RE)):
    emp=employee(emp_code); first,last=month_bounds(month); joined=max(first,parse_date(emp["joined_on"],"joined_on")); wd=sum(1 for i in range((last-joined).days+1) if (joined+timedelta(days=i)).weekday()<5) if joined<=last else 0
    pipeline=[{"$match":{"emp_code":emp_code,"date":{"$gte":first.isoformat(),"$lte":last.isoformat()}}},{"$group":{"_id":None,"leave_days":{"$sum":{"$cond":[{"$eq":["$status","LEAVE"]},1,0]}},"late_count":{"$sum":{"$cond":[{"$gt":[{"$ifNull":["$late_minutes",0]},0]},1,0]}}}}]
    stats=list(db.attendance_logs.aggregate(pipeline)); s=stats[0] if stats else {}
    presentpipe=[{"$match":{"emp_code":emp_code,"date":{"$gte":first.isoformat(),"$lte":last.isoformat()},"status":{"$in":PRESENCE}}},{"$addFields":{"dow":{"$dayOfWeek":{"$dateFromString":{"dateString":"$date"}}}}},{"$match":{"dow":{"$gte":2,"$lte":6}}},{"$group":{"_id":None,"present":{"$sum":{"$cond":[{"$eq":["$half_day",True]},0.5,1]}}}}]
    p=list(db.attendance_logs.aggregate(presentpipe)); present=rh(p[0]["present"]) if p else 0.0
    return {"emp_code":emp_code,"month":month,"working_days":wd,"present_days":present,"leave_days":int(s.get("leave_days",0)),"late_count":int(s.get("late_count",0)),"attendance_pct":rh(present/wd*100,4) if wd else None}


@app.get("/analytics/departments/summary")
def department_summary(
    month: str = Query(..., pattern=MONTH_RE),
    department: Optional[str] = None
):
    first, last = month_bounds(month)

    # Count employees who joined on or before the end of the month.
    ematch = {"joined_on": {"$lte": last.isoformat()}}

    if department is not None:
        ematch["department"] = department

    heads = list(db.employees.aggregate([
        {"$match": ematch},
        {
            "$group": {
                "_id": "$department",
                "headcount": {"$sum": 1}
            }
        },
        {"$sort": {"_id": 1}}
    ]))

    # Aggregate attendance statistics by department.
    pipe = [
        {
            "$match": {
                "date": {
                    "$gte": first.isoformat(),
                    "$lte": last.isoformat()
                }
            }
        },
        {
            "$lookup": {
                "from": "employees",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "as": "e"
            }
        },
        {"$unwind": "$e"},
        {
            "$match": {
                "e.joined_on": {"$lte": last.isoformat()},
                **({"e.department": department}
                   if department is not None else {})
            }
        },
        {
            "$group": {
                "_id": "$e.department",

                # Count present days only Monday-Friday.
                # Half-days contribute 0.5.
                "present_days": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$in": ["$status", PRESENCE]},
                                    {
                                        "$gte": [
                                            {"$dayOfWeek": {
                                                "$dateFromString": {
                                                    "dateString": "$date",
                                                    "format": "%Y-%m-%d",
                                                    "timezone": "UTC"
                                                }
                                            }},
                                            2
                                        ]
                                    },
                                    {
                                        "$lte": [
                                            {"$dayOfWeek": {
                                                "$dateFromString": {
                                                    "dateString": "$date",
                                                    "format": "%Y-%m-%d",
                                                    "timezone": "UTC"
                                                }
                                            }},
                                            6
                                        ]
                                    }
                                ]
                            },
                            {
                                "$cond": [
                                    {"$eq": ["$half_day", True]},
                                    0.5,
                                    1
                                ]
                            },
                            0
                        ]
                    }
                },

                # Average work hours use presence records
                # that have a non-null work_hours value.
                "hours_sum": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$in": ["$status", PRESENCE]},
                                    {"$ne": ["$work_hours", None]}
                                ]
                            },
                            "$work_hours",
                            0
                        ]
                    }
                },
                "hours_n": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$in": ["$status", PRESENCE]},
                                    {"$ne": ["$work_hours", None]}
                                ]
                            },
                            1,
                            0
                        ]
                    }
                },

                # Late and other counts remain independent
                # of the weekday-only present_days rule.
                "late_count": {
                    "$sum": {
                        "$cond": [
                            {
                                "$gt": [
                                    {"$ifNull": ["$late_minutes", 0]},
                                    0
                                ]
                            },
                            1,
                            0
                        ]
                    }
                },
                "total_late_minutes": {
                    "$sum": {"$ifNull": ["$late_minutes", 0]}
                },
                "leave_count": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$status", "LEAVE"]},
                            1,
                            0
                        ]
                    }
                },
                "on_duty_count": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$status", "ON_DUTY"]},
                            1,
                            0
                        ]
                    }
                }
            }
        }
    ]

    stats = {
        x["_id"]: x
        for x in db.attendance_logs.aggregate(pipe)
    }

    items = []

    for h in heads:
        s = stats.get(h["_id"], {})
        n = s.get("hours_n", 0)

        items.append({
            "department": h["_id"],
            "headcount": h["headcount"],
            "present_days": rh(s.get("present_days", 0)),
            "avg_work_hours": (
                rh(s["hours_sum"] / n) if n else None
            ),
            "late_count": int(s.get("late_count", 0)),
            "total_late_minutes": int(
                s.get("total_late_minutes", 0)
            ),
            "leave_count": int(s.get("leave_count", 0)),
            "on_duty_count": int(s.get("on_duty_count", 0))
        })

    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(month:str=Query(...,pattern=MONTH_RE),limit:int=Query(10,ge=1,le=50),department:Optional[str]=None):
    first,last=month_bounds(month); pipe=[{"$match":{"date":{"$gte":first.isoformat(),"$lte":last.isoformat()},"late_minutes":{"$gt":0}}},{"$lookup":{"from":"employees","localField":"emp_code","foreignField":"emp_code","as":"e"}},{"$unwind":"$e"}]
    if department is not None:pipe.append({"$match":{"e.department":department}})
    pipe += [{"$group":{"_id":"$emp_code","total_late_minutes":{"$sum":"$late_minutes"},"name":{"$first":"$e.name"},"department":{"$first":"$e.department"}}},{"$sort":{"total_late_minutes":-1,"_id":1}},{"$setWindowFields":{"sortBy":{"total_late_minutes":-1},"output":{"rank":{"$rank":{}}}}},{"$match":{"rank":{"$lte":limit}}},{"$sort":{"total_late_minutes":-1,"_id":1}},{"$project":{"_id":0,"emp_code":"$_id","name":1,"department":1,"total_late_minutes":1,"rank":1}}]
    return {"month":month,"items":list(db.attendance_logs.aggregate(pipe))}

def build_department_trend_pipeline(department, from_date, to_date):
    start = parse_date(from_date, "from")
    end = parse_date(to_date, "to")

    if end < start:
        raise HTTPException(422, "to must be on or after from")
    if (end - start).days + 1 > 92:
        raise HTTPException(422, "date range cannot exceed 92 days")
    if not db.employees.count_documents({"department": department}):
        raise HTTPException(404, "department not found")

    number_of_days = (end - start).days + 1
    start_datetime = datetime.combine(start, datetime.min.time())

    return [
        {"$match": {"department": department}},
        {"$limit": 1},
        {"$project": {
            "_id": 0,
            "days": {
                "$map": {
                    "input": {"$range": [0, number_of_days]},
                    "as": "offset",
                    "in": {
                        "$dateAdd": {
                            "startDate": start_datetime,
                            "unit": "day",
                            "amount": "$$offset"
                        }
                    }
                }
            }
        }},
        {"$unwind": "$days"},
        {"$project": {
            "_id": 0,
            "date": "$days",
            "day_string": {
                "$dateToString": {
                    "date": "$days",
                    "format": "%Y-%m-%d",
                    "timezone": "UTC"
                }
            }
        }},
        {"$lookup": {
            "from": "attendance_logs",
            "let": {"day": "$day_string"},
            "pipeline": [
                {"$match": {
                    "$expr": {"$eq": ["$date", "$$day"]}
                }},
                {"$lookup": {
                    "from": "employees",
                    "localField": "emp_code",
                    "foreignField": "emp_code",
                    "as": "employee"
                }},
                {"$unwind": "$employee"},
                {"$match": {
                    "employee.department": department,
                    "$expr": {
                        "$lte": ["$employee.joined_on", "$$day"]
                    }
                }},
                {"$group": {
                    "_id": None,
                    "present_count": {"$sum": {"$cond": [
                        {"$in": ["$status", PRESENCE]},
                        {"$cond": [
                            {"$eq": ["$half_day", True]},
                            0.5, 1
                        ]},
                        0
                    ]}},
                    "late_count": {"$sum": {"$cond": [
                        {"$gt": [
                            {"$ifNull": ["$late_minutes", 0]},
                            0
                        ]},
                        1, 0
                    ]}}
                }}
            ],
            "as": "daily_stats"
        }},
        {"$lookup": {
            "from": "employees",
            "let": {"day": "$day_string"},
            "pipeline": [
                {"$match": {
                    "department": department,
                    "$expr": {"$lte": ["$joined_on", "$$day"]}
                }},
                {"$count": "count"}
            ],
            "as": "headcount_stats"
        }},
        {"$set": {
            "headcount": {
                "$ifNull": [
                    {"$arrayElemAt": ["$headcount_stats.count", 0]},
                    0
                ]
            },
            "present_count": {
                "$ifNull": [
                    {"$arrayElemAt": ["$daily_stats.present_count", 0]},
                    0
                ]
            },
            "late_count": {
                "$ifNull": [
                    {"$arrayElemAt": ["$daily_stats.late_count", 0]},
                    0
                ]
            },
            "is_working_day": {
                "$and": [
                    {"$ne": [{"$dayOfWeek": "$date"}, 1]},
                    {"$ne": [{"$dayOfWeek": "$date"}, 7]}
                ]
            }
        }},
        {"$set": {
            "attendance_rate": {
                "$cond": [
                    {"$and": [
                        "$is_working_day",
                        {"$gt": ["$headcount", 0]}
                    ]},
                    {"$divide": ["$present_count", "$headcount"]},
                    None
                ]
            }
        }},
        {"$sort": {"date": 1}},
        {"$setWindowFields": {
            "sortBy": {"date": 1},
            "output": {
                "moving_avg_7d": {
                    "$avg": "$attendance_rate",
                    "window": {"documents": [-6, 0]}
                }
            }
        }},
        {"$project": {
            "_id": 0,
            "date": "$day_string",
            "is_working_day": 1,
            "headcount": 1,
            "present_count": 1,
            "late_count": 1,
            "attendance_rate": 1,
            "moving_avg_7d": 1
        }}
    ]


@app.get("/analytics/departments/{department}/trend")
def department_trend(
    department: str,
    from_date: str = Query(..., alias="from"),
    to_date: str = Query(..., alias="to")
):
    pipeline = build_department_trend_pipeline(
        department, from_date, to_date
    )

    rows = list(db.employees.aggregate(pipeline))

    for row in rows:
        row["present_count"] = rh(row["present_count"])
        if row["attendance_rate"] is not None:
            row["attendance_rate"] = rh(row["attendance_rate"], 4)
        if row["moving_avg_7d"] is not None:
            row["moving_avg_7d"] = rh(row["moving_avg_7d"], 4)

    return {
        "department": department,
        "from": from_date,
        "to": to_date,
        "items": rows
    }


@app.get("/admin/explain/{endpoint}")
def admin_explain(endpoint:str,emp_code:Optional[str]=None,month:Optional[str]=None,department:Optional[str]=None,from_date:Optional[str]=Query(None,alias="from"),to_date:Optional[str]=Query(None,alias="to"),page:int=Query(1,ge=1),page_size:int=Query(20,ge=1,le=100),limit:int=Query(10,ge=1,le=50)):
    allowed={"attendance_list","employee_monthly","department_summary","late_leaderboard","department_trend"}
    if endpoint not in allowed:raise HTTPException(422,"unsupported endpoint")
    if endpoint=="attendance_list":
        q={}
        if emp_code:q["emp_code"]=emp_code
        if from_date or to_date:
            q["date"]={}
            if from_date:q["date"]["$gte"]=parse_date(from_date,"from").isoformat()
            if to_date:q["date"]["$lte"]=parse_date(to_date,"to").isoformat()
        cmd={"find":"attendance_logs","filter":q,"sort":{"date":-1,"emp_code":1},"skip":(page-1)*page_size,"limit":page_size}
        coll="attendance_logs"
    else:
        if endpoint in {"employee_monthly","department_summary","late_leaderboard"} and not month:raise HTTPException(422,"month is required")
        if endpoint=="employee_monthly" and not emp_code:raise HTTPException(422,"emp_code is required")
        if endpoint=="department_trend" and (not department or not from_date or not to_date):raise HTTPException(422,"department, from, and to are required")
        if month: first,last=month_bounds(month)
        if endpoint=="employee_monthly":q={"emp_code":emp_code,"date":{"$gte":first.isoformat(),"$lte":last.isoformat()}}
        elif endpoint=="department_summary":q={"date":{"$gte":first.isoformat(),"$lte":last.isoformat()}}
        elif endpoint=="late_leaderboard":q={"date":{"$gte":first.isoformat(),"$lte":last.isoformat()},"late_minutes":{"$gt":0}}
        
        
        
        elif endpoint == "department_trend":
            if not department or not from_date or not to_date:
                raise HTTPException(
                    422, "department, from, and to are required"
                )

            pipeline = build_department_trend_pipeline(
                department, from_date, to_date
            )

            cmd = {
                "aggregate": "employees",
                "pipeline": pipeline,
                "cursor": {}
            }
            coll = "employees"


        else:
            raise HTTPException(422, "unsupported endpoint")

    try: explain=db.command({"explain":cmd,"verbosity":"executionStats"})
    except PyMongoError as e:raise HTTPException(500,f"explain failed: {str(e)}")
    return {
    "endpoint": endpoint,
    "collection": coll,
    "explain": json.loads(json_util.dumps(explain))
}
