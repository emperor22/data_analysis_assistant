from fastapi import UploadFile, HTTPException, Depends, Form, Request, APIRouter

from fastapi.responses import Response
from starlette.concurrency import run_in_threadpool

from app.services.dataset import (
    CsvReader,
    get_column_names_csv,
    get_row_count_csv,
)

from app.services.infra import limiter


from app.core.auth import (
    get_current_user,
)
from app.schemas.routes import (
    JoinDatasetSchema,
)


from app.services.data_transform_utils import clean_column_name, join_df_duckdb

from app.core.config import Config

from pydantic import ValidationError

router = APIRouter()


@router.post("/join_dataset")
@limiter.limit(Config.RATE_LIMIT_TASK_ENDPOINTS)
async def join_dataset(
    request: Request,
    dataset_1: UploadFile,
    dataset_2: UploadFile,
    join_dataset_data: str = Form(...),
    current_user=Depends(get_current_user),
):
    try:
        join_dataset_data = JoinDatasetSchema.model_validate_json(join_dataset_data)
    except ValidationError:
        raise HTTPException(status_code=422, detail="invalid parameters")

    join_keys = join_dataset_data.join_keys
    join_method = join_dataset_data.join_method

    dataset_1_cols = get_column_names_csv(dataset_1)
    dataset_2_cols = get_column_names_csv(dataset_2)

    dataset_1_row_count = get_row_count_csv(dataset_1)
    dataset_2_row_count = get_row_count_csv(dataset_2)

    left_on_cols_in_dataset_1_cols = all([i[0] in dataset_1_cols for i in join_keys])
    right_on_cols_in_dataset_2_cols = all([i[1] in dataset_2_cols for i in join_keys])

    if not (left_on_cols_in_dataset_1_cols and right_on_cols_in_dataset_2_cols):
        raise HTTPException(
            status_code=400, detail="all join columns must exist in both datasets"
        )

    dataset_1_size_too_big = (
        dataset_1_row_count > Config.MAX_DATAFRAME_ROWS_JOIN_UTIL
        or len(dataset_1_cols) > Config.MAX_DATAFRAME_COLS_JOIN_UTIL
    )
    dataset_2_size_too_big = (
        dataset_2_row_count > Config.MAX_DATAFRAME_ROWS_JOIN_UTIL
        or len(dataset_2_cols) > Config.MAX_DATAFRAME_COLS_JOIN_UTIL
    )

    if dataset_1_size_too_big or dataset_2_size_too_big:
        raise HTTPException(
            status_code=400, detail="the datasets dont meet the size criteria"
        )

    dataset_1_reader = CsvReader(upload_file=dataset_1)
    dataset_1_data = await run_in_threadpool(dataset_1_reader.get_dataframe_dict)
    dataset_1_df = dataset_1_data["dataframe"]

    dataset_2_reader = CsvReader(upload_file=dataset_2)
    dataset_2_data = await run_in_threadpool(dataset_2_reader.get_dataframe_dict)
    dataset_2_df = dataset_2_data["dataframe"]

    join_keys_clean = [
        (clean_column_name(i), clean_column_name(j)) for i, j in join_keys
    ]

    joined_df = await run_in_threadpool(
        join_df_duckdb,
        df1=dataset_1_df,
        df2=dataset_2_df,
        join_keys=join_keys_clean,
        how=join_method,
    )
    joined_df_buffer = joined_df.to_csv(index=False, compression="gzip")

    content = joined_df_buffer.getvalue()
    headers = {"Content-Disposition": 'attachment; filename="result_dataset.csv.gz"'}

    return Response(content=content, headers=headers, media_type="application/gzip")
