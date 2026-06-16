from sqlalchemy import text, create_engine
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from sqlalchemy import ForeignKey, Integer, String, Text, DateTime, Column, Boolean

from sqlalchemy.orm import declarative_base

from app.core.config import Config



base_engine = create_async_engine(
    Config.DATABASE_URL_ASYNC,
    pool_size=10,
    max_overflow=2,
    pool_recycle=300,
    pool_pre_ping=True,
    pool_use_lifo=True,
)
base_engine_sync = create_engine(
    Config.DATABASE_URL_SYNC,
    pool_size=10,
    max_overflow=2,
    pool_recycle=300,
    pool_pre_ping=True,
    pool_use_lifo=True,
)

SessionLocal = async_sessionmaker(
    bind=base_engine, expire_on_commit=False, class_=AsyncSession, autoflush=False
)

Base = declarative_base()


class AppUsers(Base):
    __tablename__ = "app_user"

    id = Column(Text, primary_key=True)
    username = Column(String, unique=True, nullable=False)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    email = Column(String, unique=True, nullable=False)
    otp = Column(String, nullable=True)
    otp_expire = Column(DateTime(timezone=True), nullable=True)
    last_otp_request = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True))
    api_key_cerebras = Column(String)
    api_key_google = Column(String)
    api_key_openrouter = Column(String)


class PromptAndResult(Base):
    __tablename__ = "prompt_and_result"

    id = Column(Text, primary_key=True)

    user_id = Column(Text, ForeignKey("app_user.id"))

    run_name = Column(String)
    prompt_version = Column(Text)
    filename = Column(Text)
    dataset_cols = Column(Text)
    model = Column(String)
    prompt_result = Column(Text)
    additional_analyses_prompt_result = Column(Text)
    status = Column(String)
    celery_task_id = Column(String)
    created_at = Column(DateTime(timezone=True))
    rate_limit_retry_count = Column(Integer, nullable=True, server_default=text("0"))
    last_accessed_at = Column(DateTime(timezone=True))


class TaskRun(Base):
    __tablename__ = "task_run"

    request_id = Column(Text, ForeignKey("prompt_and_result.id"), primary_key=True)
    user_id = Column(Text, ForeignKey("app_user.id"), primary_key=True)

    original_common_tasks = Column(Text)
    common_tasks_w_result = Column(Text)
    column_transforms_status = Column(String)
    column_combinations_status = Column(String)
    columns_info = Column(Text)
    celery_task_id = Column(String)
    final_dataset_snippet = Column(Text)
    created_at = Column(DateTime(timezone=True))


class BlacklistedDatasets(Base):
    __tablename__ = "blacklisted_datasets"

    dataset_id = Column(Text, primary_key=True)
    reason = Column(Text)
    failed_attempts = Column(Integer)
    last_failed_at = Column(DateTime(timezone=True))
    is_blacklisted = Column(Boolean)


class UserCustomizedTasks(Base):
    __tablename__ = "user_customized_tasks"

    request_id = Column(Text, ForeignKey("prompt_and_result.id"), primary_key=True)
    user_id = Column(Text, ForeignKey("app_user.id"), primary_key=True)

    saved_tasks_slot_1 = Column(Text)
    saved_tasks_slot_2 = Column(Text)
    saved_tasks_slot_3 = Column(Text)

    imported_original_tasks = Column(Text)

