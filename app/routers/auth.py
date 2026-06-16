from fastapi import Depends, APIRouter, HTTPException, Request

from app.services.infra import get_background_tasks, limiter


from app.tasks import (
    send_email_task,
)
from app.crud import (
    UserTableOperation,
    get_user_table_ops,
)
from app.auth import (
    create_access_token,
    generate_random_otp,
    verify_otp,
)
from app.schemas import (
    UserRegisterSchema,
    GetOTPSchema,
    LoginSchema,
)

from app.config import Config


from app.logger import logger


from sqlalchemy.exc import IntegrityError


from datetime import datetime, timezone, timedelta


router = APIRouter()


@router.post("/get_otp")
async def get_otp(
    get_otp_data: GetOTPSchema,
    background_tasks=Depends(get_background_tasks),
    user_table_ops: UserTableOperation = Depends(get_user_table_ops),
):
    username = get_otp_data.username

    user = await user_table_ops.get_user(username)

    if not user:
        logger.warning(f"user {username} does not exist in db and tried to log in")
        raise HTTPException(status_code=401, detail="Invalid credentials.")

    if user["last_otp_request"] is not None:
        if datetime.now(timezone.utc) < user["last_otp_request"] + timedelta(
            minutes=1
        ):  # if one minute has not passed since the last otp request
            raise HTTPException(
                status_code=429,
                detail="You need to wait one minute before requesting another OTP",
            )

    raw_otp, encrypted_otp = generate_random_otp()
    otp_expire = datetime.now(timezone.utc) + timedelta(
        minutes=Config.OTP_EXPIRE_MINUTES
    )

    await user_table_ops.update_otp(
        username=username, otp=encrypted_otp, otp_expire=otp_expire
    )

    receiver = user["email"]
    subject = "OTP for Data Analysis Assistant app"
    body = f"Your OTP is {raw_otp}"

    send_email_task.delay(subject=subject, receiver=receiver, body=body)

    return {"detail": "otp has been sent"}


@router.post("/login")
@limiter.limit(Config.RATE_LIMIT_LOGIN)
async def login(
    request: Request,
    login_data: LoginSchema,
    user_table_ops: UserTableOperation = Depends(get_user_table_ops),
):
    user = await user_table_ops.get_user(login_data.username)

    if not user:
        logger.warning(
            f"user {login_data.username} does not exist in db and tried to log in"
        )
        raise HTTPException(status_code=401, detail="Invalid credentials.")

    user_otp = user["otp"]
    username = user["username"]
    otp_expire = user["otp_expire"]

    if isinstance(otp_expire, str):
        otp_expire = datetime.fromisoformat(otp_expire)

    if not verify_otp(login_data.otp, user_otp):
        logger.warning(f"user {login_data.username} failed to log in")
        raise HTTPException(status_code=401, detail="Incorrect username")

    if datetime.now(timezone.utc) > otp_expire:
        raise HTTPException(
            status_code=401, detail="Expired OTP. Please generate a new one."
        )

    access_token = create_access_token(
        data={"sub": username}, expire_minutes=Config.ACCESS_TOKEN_EXPIRE_MINUTES
    )

    logger.info(f"user {username} logged in")

    # generate new otp to invalidate previous otp
    _, encrypted_otp = generate_random_otp()
    otp_expire = datetime.now(timezone.utc) + timedelta(
        minutes=Config.OTP_EXPIRE_MINUTES
    )

    await user_table_ops.update_otp(
        username=username, otp=encrypted_otp, otp_expire=otp_expire
    )

    return {"access_token": access_token, "token_type": "bearer"}


@router.post("/register_user")
@limiter.limit(Config.RATE_LIMIT_REGISTER)
async def register_user(
    request: Request,
    user_register_data: UserRegisterSchema,
    user_table_ops: UserTableOperation = Depends(get_user_table_ops),
):
    try:
        username = user_register_data.username
        email = user_register_data.email
        first_name = user_register_data.first_name
        last_name = user_register_data.last_name

        await user_table_ops.create_user(
            username=username, email=email, first_name=first_name, last_name=last_name
        )

        logger.info(f"account {username} successfully created")

        return {"detail": f"account {username} successfully created"}

    except IntegrityError:
        logger.warning(
            f"{username}/{email} failed to register because of conflicting username/email."
        )
        raise HTTPException(
            status_code=409,
            detail=f"username {username} or email {email} already exists.",
        )
