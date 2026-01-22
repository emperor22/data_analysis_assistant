from pydantic import BaseModel
from fastapi.security import OAuth2PasswordBearer
from fastapi.exceptions import HTTPException
from fastapi import Depends, status
from jwt.exceptions import InvalidTokenError
import jwt
import asyncio
from passlib.context import CryptContext

from datetime import datetime, timezone, timedelta

from app.crud import UserTableOperation, get_session
from app.schemas import GetCurrentUserModel
from app.config import Config

import secrets

import os
import hmac
import hashlib
import base64

from app.logger import logger



def generate_random_otp():
    secret_key = Config.SECRET_KEY_OTP_ENCRYPT.encode()
    raw_otp = str(secrets.randbelow(1000000)).zfill(6)
    digest = hmac.new(secret_key, raw_otp.encode(), hashlib.sha256).digest()
    encrypted_otp = base64.b64encode(digest).decode()
    return raw_otp, encrypted_otp

def verify_otp(input_otp, encrypted_stored_otp):
    secret_key = Config.SECRET_KEY_OTP_ENCRYPT.encode()
    expected = hmac.new(secret_key, input_otp.encode(), hashlib.sha256).digest()
    stored = base64.b64decode(encrypted_stored_otp)
    return hmac.compare_digest(expected, stored)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def create_access_token(data: dict, expire_minutes=None):
    to_encode = data.copy()
    expire_minutes = 120 if not expire_minutes else expire_minutes
    expire = datetime.now(timezone.utc) + timedelta(minutes=expire_minutes)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, Config.SECRET_KEY, algorithm=Config.ALGORITHM)
    return encoded_jwt


    
async def get_current_user(token=Depends(oauth2_scheme), conn=Depends(get_session)):
    credentials_exception = HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Could not validate credentials", 
                                          headers={"WWW-Authenticate": "Bearer"})
    try:
        payload = jwt.decode(token, Config.SECRET_KEY, algorithms=[Config.ALGORITHM])
        username = payload.get("sub")
        if username is None:
            raise credentials_exception
        
    except InvalidTokenError:
        raise credentials_exception
    
    user_table_ops = UserTableOperation(conn)
    user = await user_table_ops.get_user(username)

    if user is None:
        raise credentials_exception

    return GetCurrentUserModel(username=user['username'], user_id=user['id'], email=user['email'])

def get_admin(current_user=Depends(get_current_user)):
    logger.debug('caught')
    user_email = current_user.email
    if not user_email == Config.ADMIN_EMAIL:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="you need admin privileges to access this")
    return



    

    
    


