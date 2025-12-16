from pydantic import BaseModel
from fastapi.security import OAuth2PasswordBearer
from fastapi.exceptions import HTTPException
from fastapi import Depends, status
from jwt.exceptions import InvalidTokenError
import jwt
import asyncio
from passlib.context import CryptContext





from datetime import datetime, timezone, timedelta

from app.crud import UserTableOperation, get_conn
from app.schemas import GetCurrentUserModel

import secrets

import os, hmac, hashlib, base64

SECRET_KEY = '6023ea54cbd56eed9d88d6ae008c6a14'
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 120



SECRET_KEY_OTP_ENCRYPT = 'satuduatiga'

def generate_random_otp():
    secret_key = SECRET_KEY_OTP_ENCRYPT.encode()
    raw_otp = str(secrets.randbelow(1000000)).zfill(6)
    digest = hmac.new(secret_key, raw_otp.encode(), hashlib.sha256).digest()
    encrypted_otp = base64.b64encode(digest).decode()
    return raw_otp, encrypted_otp

def verify_otp(raw_otp, encrypted_otp):
    secret_key = SECRET_KEY_OTP_ENCRYPT.encode()
    expected = hmac.new(secret_key, raw_otp.encode(), hashlib.sha256).digest()
    stored = base64.b64decode(encrypted_otp)
    return hmac.compare_digest(expected, stored)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def create_access_token(data: dict, expire_minutes=None):
    to_encode = data.copy()
    expire_minutes = 120 if not expire_minutes else expire_minutes
    expire = datetime.now(timezone.utc) + timedelta(minutes=expire_minutes)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


    
def get_current_user(token=Depends(oauth2_scheme), conn=Depends(get_conn)):
    credentials_exception = HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Could not validate credentials", 
                                          headers={"WWW-Authenticate": "Bearer"})
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if username is None:
            raise credentials_exception
        
    except InvalidTokenError:
        raise credentials_exception
    
    user_table_ops = UserTableOperation(conn)
    user = asyncio.run(user_table_ops.get_user(username))

    if user is None:
        raise credentials_exception

    return GetCurrentUserModel(username=user['username'], user_id=user['id'], email=user['email'])



    

    
    


