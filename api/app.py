from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
import sqlite3
import os
import jwt
import time
from datetime import datetime, timedelta

app = FastAPI()

# config logs pour le troubleshoot de l'API (mode "a" = append)
log_file = open("app_logs.txt", "a", buffering=1)
log_file.write(f"\n--- API Démarrée à {time.time()} ---\n")

SECRET_KEY = "key_projet_yellow_cabs"
ALGORITHM = "HS256"

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/token")

def get_db():
    # connexion a sqlite
    db_path = os.path.join(os.path.dirname(__file__), '..', 'pipeline', 'datamarts.db')
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(hours=1)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

@app.post("/auth/token")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    log_file.write(f"Tentative de connexion : {form_data.username}\n")
    
    # juste admin admin pour le test
    if form_data.username == "admin" and form_data.password == "admin":
        access_token = create_access_token(data={"sub": form_data.username})
        log_file.write("-> Connexion reussie.\n")
        return {"access_token": access_token, "token_type": "bearer"}
        
    log_file.write("-> Echec de la connexion.\n")
    raise HTTPException(status_code=400, detail="mauvais login ou mot de passe")

def verify_token(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except:
        log_file.write("-> Erreur : Token invalide ou expire.\n")
        raise HTTPException(status_code=401, detail="token invalide")

# api datamarts

@app.get("/datamarts/zone_performance")
def get_zone_performance(skip: int = 0, limit: int = 10, db: sqlite3.Connection = Depends(get_db), token: str = Depends(verify_token)):
    log_file.write(f"Requete API : zone_performance (limit={limit}, skip={skip})\n")
    cur = db.cursor()
    cur.execute("SELECT * FROM dm_zone_performance LIMIT ? OFFSET ?", (limit, skip))
    res = cur.fetchall()
    return {"data": [dict(r) for r in res]}

@app.get("/datamarts/hourly_demand")
def get_hourly_demand(skip: int = 0, limit: int = 10, db: sqlite3.Connection = Depends(get_db), token: str = Depends(verify_token)):
    log_file.write(f"Requete API : hourly_demand (limit={limit}, skip={skip})\n")
    cur = db.cursor()
    cur.execute("SELECT * FROM dm_hourly_demand LIMIT ? OFFSET ?", (limit, skip))
    res = cur.fetchall()
    return {"data": [dict(r) for r in res]}

@app.get("/datamarts/payment_analysis")
def get_payment_analysis(skip: int = 0, limit: int = 10, db: sqlite3.Connection = Depends(get_db), token: str = Depends(verify_token)):
    log_file.write(f"Requete API : payment_analysis (limit={limit}, skip={skip})\n")
    cur = db.cursor()
    cur.execute("SELECT * FROM dm_payment_analysis LIMIT ? OFFSET ?", (limit, skip))
    res = cur.fetchall()
    return {"data": [dict(r) for r in res]}

@app.get("/")
def home():
    log_file.write("Visite sur la page d'accueil de l'API.\n")
    return {"msg": "api projet yellow cabs"}
