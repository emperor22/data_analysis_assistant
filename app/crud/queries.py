from sqlalchemy import text

import uuid

from datetime import datetime, date, timedelta

from app.services.utils import get_current_time_utc
from app.schemas.enums import TaskStatus

import json


class BlacklistedDatasetsTableOperation:
    def __init__(self, conn_sync):
        self.conn_sync = conn_sync
        self.table_name = "blacklisted_datasets"

    def add_dataset_to_table(self, dataset_id):

        query = f"""insert into {self.table_name}(dataset_id, reason, failed_attempts, last_failed_at, is_blacklisted) 
                    values (:dataset_id, :reason, 0, :last_failed_at, false)"""
        self.conn_sync.execute(
            text(query),
            {
                "dataset_id": dataset_id,
                "reason": "",
                "last_failed_at": get_current_time_utc(),
            },
        )
        self.conn_sync.commit()

    def remove_dataset_from_table(self, dataset_id):
        query = f"""delete from {self.table_name} where dataset_id = :dataset_id"""
        self.conn_sync.execute(text(query), {"dataset_id": dataset_id})
        self.conn_sync.commit()

    def increment_failed_attempt(self, dataset_id):
        cur_failed_attempt = self.get_failed_attempt_count(dataset_id)

        query = f"""update {self.table_name}
                   set failed_attempts = :new_failed_attempts
                   where dataset_id = :dataset_id"""
        self.conn_sync.execute(
            text(query),
            {"dataset_id": dataset_id, "new_failed_attempts": cur_failed_attempt + 1},
        )
        self.conn_sync.commit()

    def reset_failed_attempt_count(self, dataset_id):
        query = f"""update {self.table_name}
                   set failed_attempts = 0
                   where dataset_id = :dataset_id"""
        self.conn_sync.execute(text(query), {"dataset_id": dataset_id})
        self.conn_sync.commit()

    def get_failed_attempt_count(self, dataset_id):
        query = f"""select failed_attempts from {self.table_name} where dataset_id = :dataset_id"""

        res = self.conn_sync.execute(text(query), {"dataset_id": dataset_id})
        res = res.fetchone()
        return res._mapping["failed_attempts"] if res else None

    def check_if_blacklisted(self, dataset_id):
        query = f"""select is_blacklisted from {self.table_name} where dataset_id = :dataset_id"""

        res = self.conn_sync.execute(text(query), {"dataset_id": dataset_id})
        res = res.fetchone()

        if not res:
            return None

        return bool(res._mapping["is_blacklisted"])

    def mark_as_blacklisted(self, dataset_id):
        query = f"""update {self.table_name}
                   set is_blacklisted = true
                   where dataset_id = :dataset_id"""
        self.conn_sync.execute(text(query), {"dataset_id": dataset_id})
        self.conn_sync.commit()


class UserCustomizedTasksTableOperation:
    def __init__(self, conn):
        self.conn = conn
        self.table_name = "user_customized_tasks"

    async def delete_task(self, user_id, request_id):
        query = f"""delete from {self.table_name} where user_id = :user_id and request_id = :request_id"""

        await self.conn.execute(
            text(query), {"user_id": user_id, "request_id": request_id}
        )

    async def fetch_customized_tasks(self, user_id, request_id, slot):
        col = f"saved_tasks_slot_{slot}"
        allowed_cols = [
            "saved_tasks_slot_1",
            "saved_tasks_slot_2",
            "saved_tasks_slot_3",
        ]

        if col not in allowed_cols:
            return None

        query = f"""select {col} from {self.table_name} where user_id = :user_id and request_id = :request_id"""

        res = await self.conn.execute(
            text(query), {"user_id": user_id, "request_id": request_id}
        )
        res = res.fetchone()
        return res._mapping[col] if res else None

    async def fetch_imported_tasks(self, user_id, request_id):
        query = f"""select imported_original_tasks from {self.table_name} where user_id = :user_id and request_id = :request_id"""

        res = await self.conn.execute(
            text(query), {"user_id": user_id, "request_id": request_id}
        )
        res = res.fetchone()
        return res._mapping["imported_original_tasks"] if res else None

    async def check_if_customized_tasks_empty(self, user_id, request_id):
        query = f"""select saved_tasks_slot_1, saved_tasks_slot_2, saved_tasks_slot_3 from {self.table_name} where user_id = :user_id and request_id = :request_id"""

        res = await self.conn.execute(
            text(query), {"user_id": user_id, "request_id": request_id}
        )
        res = res.fetchone()
        values = res._mapping.values()
        return all([i is None for i in values])

    async def update_customized_tasks(self, user_id, request_id, slot, tasks):
        col = f"saved_tasks_slot_{slot}"
        allowed_cols = [
            "saved_tasks_slot_1",
            "saved_tasks_slot_2",
            "saved_tasks_slot_3",
        ]

        if col not in allowed_cols:
            return None

        query = f"""update {self.table_name} set {col} = :tasks where user_id = :user_id and request_id = :request_id"""

        await self.conn.execute(
            text(query), {"user_id": user_id, "request_id": request_id, "tasks": tasks}
        )

    async def delete_customized_tasks(self, user_id, request_id, slot):
        await self.update_customized_tasks(
            user_id=user_id, request_id=request_id, slot=slot, tasks=None
        )

    async def set_imported_tasks(self, user_id, request_id, imported_task_ids: list):
        imported_task_ids = json.dumps(imported_task_ids)
        query = f"""update {self.table_name} set imported_original_tasks = :imported_task_ids where user_id = :user_id and request_id = :request_id"""

        await self.conn.execute(
            text(query),
            {
                "user_id": user_id,
                "request_id": request_id,
                "imported_task_ids": imported_task_ids,
            },
        )

    async def add_request_id_to_table(self, user_id, request_id):
        query = f"""insert into {self.table_name}(user_id, request_id) 
                    values (:user_id, :request_id)"""
        await self.conn.execute(
            text(query), {"user_id": user_id, "request_id": request_id}
        )


class UserTableOperation:
    def __init__(self, conn=None, conn_sync=None):
        self.conn = conn
        self.conn_sync = conn_sync
        self.table_name = "app_user"

    async def add_api_key(self, user_id, key, provider):

        query = f"""update {self.table_name} set api_key_{provider} = :key where id = :user_id"""

        await self.conn.execute(text(query), {"key": key, "user_id": user_id})

    async def delete_api_key(self, user_id, provider):

        query = f"""update {self.table_name} set api_key_{provider} = NULL where id = :user_id"""

        await self.conn.execute(text(query), {"user_id": user_id})

    async def get_api_key(self, user_id, provider):

        col = f"api_key_{provider}"
        query = f"""select {col} from {self.table_name} where id = :user_id"""
        res = await self.conn.execute(text(query), {"user_id": user_id})
        res = res.fetchone()

        return res._mapping[col] if res else None

    async def get_user(self, username):
        query = f"""select username, id, email, last_otp_request, otp, otp_expire from {self.table_name} where username = :username"""

        res = await self.conn.execute(text(query), {"username": username})
        res = res.fetchone()
        return res._mapping if res else None

    async def create_user(self, username, email, first_name, last_name):
        user_id = str(uuid.uuid4())

        query = f"""insert into {self.table_name}(id, username, email, first_name, last_name, created_at) 
                    values (:user_id, :username, :email, :first_name, :last_name, :created_at)"""
        await self.conn.execute(
            text(query),
            {
                "user_id": user_id,
                "username": username,
                "email": email,
                "first_name": first_name,
                "last_name": last_name,
                "created_at": get_current_time_utc(),
            },
        )

    async def update_otp(self, username: str, otp: str, otp_expire: datetime):
        query = f"""update {self.table_name}
                   set otp = :otp, otp_expire = :otp_expire, last_otp_request = :last_otp_request
                   where username = :username"""
        await self.conn.execute(
            text(query),
            {
                "otp": otp,
                "otp_expire": otp_expire,
                "last_otp_request": get_current_time_utc(),
                "username": username,
            },
        )

    async def delete_user(self, username):
        query = f"""delete from {self.table_name} where username = :username """

        await self.conn.execute(text(query), {"username": username})


class PromptTableOperation:
    def __init__(self, conn=None, conn_sync=None):
        self.conn = conn
        self.conn_sync = conn_sync
        self.table_name = "prompt_and_result"

    async def add_task(
        self,
        user_id: str,
        run_name: str,
        prompt_version: str,
        filename: str,
        dataset_cols: str,
        model: str,
    ):

        req_id = str(uuid.uuid4())
        query = f"""insert into {self.table_name}(id, user_id, run_name, prompt_version, filename, dataset_cols, model, created_at, status, last_accessed_at)
                    values (:id, :user_id, :run_name, :prompt_version, :filename, :dataset_cols, :model, :created_at, :status, :last_accessed_at)"""
        await self.conn.execute(
            text(query),
            {
                "id": req_id,
                "user_id": user_id,
                "prompt_version": str(prompt_version),
                "run_name": run_name,
                "filename": filename,
                "dataset_cols": dataset_cols,
                "model": model,
                "created_at": get_current_time_utc(),
                "status": TaskStatus.task_queued.value,
                "last_accessed_at": get_current_time_utc(),
            },
        )

        return req_id

    async def delete_task(self, user_id, request_id):
        query = f"""delete from {self.table_name} where user_id = :user_id and id = :request_id"""

        await self.conn.execute(
            text(query), {"user_id": user_id, "request_id": request_id}
        )

    # is a synchronous function because will be used in gevent worker
    def insert_prompt_result_sync(self, request_id: str, prompt_result: str):

        query = f"""update {self.table_name}
                   set prompt_result = :prompt_result
                   where id = :request_id"""

        self.conn_sync.execute(
            text(query), {"request_id": request_id, "prompt_result": prompt_result}
        )
        self.conn_sync.commit()

    async def get_prompt_result(self, request_id: str):
        query = (
            f"""select prompt_result from {self.table_name} where id = :request_id"""
        )

        res = await self.conn.execute(text(query), {"request_id": request_id})
        res = res.fetchone()
        return res._mapping if res and res.prompt_result is not None else None

    def get_prompt_result_sync(self, request_id: str, user_id: str):

        query = f"""select prompt_result from {self.table_name} where user_id = :user_id and id = :request_id"""

        res = self.conn_sync.execute(
            text(query), {"request_id": request_id, "user_id": user_id}
        )
        res = res.fetchone()
        return res._mapping if res and res.prompt_result is not None else None

    def insert_additional_analyses_prompt_result_sync(
        self, request_id: str, additional_analyses_prompt_result: str
    ):

        query = f"""update {self.table_name}
                   set additional_analyses_prompt_result = :additional_analyses_prompt_result
                   where id = :request_id"""

        self.conn_sync.execute(
            text(query),
            {
                "request_id": request_id,
                "additional_analyses_prompt_result": additional_analyses_prompt_result,
            },
        )
        self.conn_sync.commit()

    async def get_additional_analyses_prompt_result(
        self, request_id: str, user_id: str
    ):
        query = f"""select additional_analyses_prompt_result from {self.table_name} where user_id = :user_id and id = :request_id"""

        res = await self.conn.execute(
            text(query), {"request_id": request_id, "user_id": user_id}
        )
        res = res.fetchone()
        return (
            res._mapping
            if res and res.additional_analyses_prompt_result is not None
            else None
        )

    def change_request_status_sync(self, request_id, status):

        query = f"""update {self.table_name}
                   set status = :status
                   where id = :request_id"""

        self.conn_sync.execute(
            text(query), {"request_id": request_id, "status": status}
        )
        self.conn_sync.commit()

    async def get_request_status(self, request_id: str, user_id: str):
        query = f"""select status from {self.table_name} where user_id = :user_id and id = :request_id"""

        res = await self.conn.execute(
            text(query), {"request_id": request_id, "user_id": user_id}
        )
        res = res.fetchone()
        return res._mapping if res and res.status is not None else None

    async def get_dataset_filename(self, request_id: str, user_id: str):
        query = f"""select filename from {self.table_name} where user_id = :user_id and id = :request_id"""

        res = await self.conn.execute(
            text(query), {"request_id": request_id, "user_id": user_id}
        )
        res = res.fetchone()
        return res._mapping["filename"] if res else None

    async def get_run_name(self, request_id: str, user_id: str):
        query = f"""select run_name from {self.table_name} where user_id = :user_id and id = :request_id"""

        res = await self.conn.execute(
            text(query), {"request_id": request_id, "user_id": user_id}
        )
        res = res.fetchone()
        return res._mapping["run_name"] if res else None

    async def get_request_ids_by_user(self, user_id: str):
        query = f"""select id, run_name, filename, created_at, status from {self.table_name} where user_id = :user_id"""
        res = await self.conn.execute(text(query), {"user_id": user_id})
        res = res.fetchall()
        return (
            [(i.id, i.run_name, i.filename, i.created_at, i.status) for i in res]
            if res
            else None
        )

    async def get_dataset_columns_by_id(self, request_id: str, user_id: str):
        query = f"""select dataset_cols from {self.table_name} where user_id = :user_id and id = :request_id"""

        res = await self.conn.execute(
            text(query), {"request_id": request_id, "user_id": user_id}
        )
        res = res.fetchone()
        return (
            res._mapping["dataset_cols"]
            if res and res.dataset_cols is not None
            else None
        )

    def update_last_accessed_column_sync(self, update_dct):

        update_dct = {
            i: datetime.strptime(j, "%Y-%m-%d") for i, j in update_dct.items()
        }  # update dct is {req_id: date_str}
        for req_id, dt in update_dct.items():
            query = f"""update {self.table_name} set last_accessed_at = :date where id = :req_id"""
            self.conn_sync.execute(
                text(query), {"date": dt, "req_id": req_id}
            )  # no explicit commit because its used with conn.begin()

    def get_least_accessed_request_ids_sync(self, thres_days):

        date_filt = (date.today() - timedelta(days=thres_days)).strftime("%Y-%m-%d")

        query = (
            f"""select id from {self.table_name} where last_accessed_at < :date_filt"""
        )

        res = self.conn_sync.execute(text(query), {"date_filt": date_filt})
        res = res.fetchall()
        return [row._mapping["id"] for row in res] if res else None

    def increment_rate_limit_retry_count_sync(self, user_id, request_id):

        current_retry_count = self.check_rate_limit_retry_count(user_id, request_id)

        query = f"""update {self.table_name} set rate_limit_retry_count = :new_count where user_id = :user_id and id = :request_id"""
        self.conn_sync.execute(
            text(query),
            {
                "user_id": user_id,
                "request_id": request_id,
                "new_count": current_retry_count + 1,
            },
        )
        self.conn_sync.commit()

    def check_rate_limit_retry_count(self, user_id, request_id):

        query = f"""select rate_limit_retry_count from {self.table_name} where user_id = :user_id and id = :request_id"""
        res = self.conn_sync.execute(
            text(query), {"user_id": user_id, "request_id": request_id}
        )
        res = res.fetchone()

        return res._mapping["rate_limit_retry_count"] if res else 0

    def reset_rate_limit_retry_count_sync(self, user_id, request_id):

        query = f"""update {self.table_name} set rate_limit_retry_count = 0 where user_id = :user_id and id = :request_id"""
        self.conn_sync.execute(
            text(query), {"user_id": user_id, "request_id": request_id}
        )
        self.conn_sync.commit()


class TaskRunTableOperation:
    def __init__(self, conn=None, conn_sync=None):
        self.conn = conn
        self.conn_sync = conn_sync
        self.table_name = "task_run"

    async def empty_modified_task_result(self, request_id: str, user_id: str):
        query = f"""update {self.table_name} 
                    set common_tasks_w_result = NULL
                    where user_id = :user_id and request_id = :request_id"""

        await self.conn.execute(
            text(query), {"request_id": request_id, "user_id": user_id}
        )

    async def delete_task(self, user_id, request_id):
        query = f"""delete from {self.table_name} where user_id = :user_id and request_id = :request_id"""

        await self.conn.execute(
            text(query), {"user_id": user_id, "request_id": request_id}
        )

    def add_task_result_sync(self, request_id: str, user_id: str):

        query = f"""insert into {self.table_name}(request_id, user_id, created_at) values (:request_id, :user_id, :created_at)"""

        self.conn_sync.execute(
            text(query),
            {
                "request_id": request_id,
                "user_id": user_id,
                "created_at": get_current_time_utc(),
            },
        )
        self.conn_sync.commit()

    def update_task_result_sync(self, request_id: str, tasks: str):

        query = f"""update {self.table_name}
                   set common_tasks_w_result = :tasks
                   where request_id = :request_id"""

        self.conn_sync.execute(text(query), {"request_id": request_id, "tasks": tasks})
        self.conn_sync.commit()

    def update_original_common_task_result_sync(self, request_id: str, tasks: str):

        query = f"""update {self.table_name}
                   set original_common_tasks = :tasks
                   where request_id = :request_id"""

        self.conn_sync.execute(text(query), {"request_id": request_id, "tasks": tasks})
        self.conn_sync.commit()

    def update_column_transform_task_status_sync(
        self, request_id, column_transforms_status
    ):

        query = f"""update {self.table_name}
            set column_transforms_status = :column_transforms_status
            where request_id = :request_id"""

        self.conn_sync.execute(
            text(query),
            {
                "request_id": request_id,
                "column_transforms_status": column_transforms_status,
            },
        )
        self.conn_sync.commit()

    def update_column_combination_task_status_sync(
        self, request_id, column_combinations_status
    ):

        query = f"""update {self.table_name}
            set column_combinations_status = :column_combinations_status
            where request_id = :request_id"""

        self.conn_sync.execute(
            text(query),
            {
                "request_id": request_id,
                "column_combinations_status": column_combinations_status,
            },
        )
        self.conn_sync.commit()

    def update_final_dataset_snippet_sync(self, request_id, dataset_snippet):

        query = f"""update {self.table_name}
            set final_dataset_snippet = :dataset_snippet
            where request_id = :request_id"""

        self.conn_sync.execute(
            text(query), {"request_id": request_id, "dataset_snippet": dataset_snippet}
        )
        self.conn_sync.commit()

    def update_columns_info_sync(self, request_id, columns_info):

        query = f"""update {self.table_name}
            set columns_info = :columns_info
            where request_id = :request_id"""

        self.conn_sync.execute(
            text(query), {"request_id": request_id, "columns_info": columns_info}
        )
        self.conn_sync.commit()

    async def get_original_tasks_by_id(self, user_id: int, request_id: str):
        query = f"""select original_common_tasks from {self.table_name} 
                   where user_id = :user_id and request_id = :request_id"""

        res = await self.conn.execute(
            text(query), {"user_id": user_id, "request_id": request_id}
        )
        res = res.fetchone()
        return res._mapping if res and res.original_common_tasks is not None else None

    async def get_columns_combinations_by_id(self, user_id: int, request_id: str):
        query = f"""select column_combinations_status from {self.table_name} 
                   where user_id = :user_id and request_id = :request_id"""

        res = await self.conn.execute(
            text(query), {"user_id": user_id, "request_id": request_id}
        )
        res = res.fetchone()
        return res._mapping if res else None

    async def get_columns_transformations_by_id(self, user_id: int, request_id: str):
        query = f"""select column_transforms_status from {self.table_name} 
                   where user_id = :user_id and request_id = :request_id"""

        res = await self.conn.execute(
            text(query), {"user_id": user_id, "request_id": request_id}
        )
        res = res.fetchone()
        return res._mapping if res else None

    async def get_modified_tasks_by_id(self, user_id: int, request_id: str):
        query = f"""select common_tasks_w_result from {self.table_name} 
                   where user_id = :user_id and request_id = :request_id"""

        res = await self.conn.execute(
            text(query), {"user_id": user_id, "request_id": request_id}
        )
        res = res.fetchone()
        return res._mapping if res and res.common_tasks_w_result is not None else None

    async def get_columns_info_by_id(self, user_id: int, request_id: str):
        query = f"""select columns_info from {self.table_name} 
                   where user_id = :user_id and request_id = :request_id"""

        res = await self.conn.execute(
            text(query), {"user_id": user_id, "request_id": request_id}
        )
        res = res.fetchone()
        return res._mapping if res else None

    async def get_dataset_snippet_by_id(self, user_id: int, request_id: str):
        query = f"""select final_dataset_snippet from {self.table_name} 
                   where user_id = :user_id and request_id = :request_id"""

        res = await self.conn.execute(
            text(query), {"user_id": user_id, "request_id": request_id}
        )
        res = res.fetchone()
        return res._mapping if res else None

    def get_task_by_id_sync(self, user_id: int, request_id: str):
        query = f"""select original_common_tasks from {self.table_name} where user_id = :user_id and request_id = :request_id"""

        res = self.conn_sync.execute(
            text(query), {"user_id": user_id, "request_id": request_id}
        )
        res = res.fetchone()
        return res._mapping["original_common_tasks"] if res else None

    def request_id_exists(self, request_id: str):
        query = f"""select request_id from {self.table_name} where request_id = :request_id"""

        res = self.conn_sync.execute(text(query), {"request_id": request_id})
        res = res.fetchone()
        return res is not None
