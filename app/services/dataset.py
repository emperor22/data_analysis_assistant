from app.services.data_transform_utils import (
    clean_dataset,
    get_granularity_map,
    get_dataset_id,
)
from app.core.exceptions import InvalidDatasetException, FileReadException
from app.core.config import Config
from app.schemas.enums import TaskProcessingRunType

from abc import ABC, abstractmethod
from fastapi import UploadFile

import pandas as pd
import io
import csv
import os
import math


class FileReader(ABC):
    def __init__(self, upload_file: UploadFile):
        self.file_content = upload_file.file.read()
        self.filename = upload_file.filename or "uploaded_file"
        self.granularity_map = {}
        self.df: pd.DataFrame | None = None

        if not self.file_content:
            raise InvalidDatasetException("The uploaded file is empty.")

    def get_dataframe_dict(self):
        self._read_file()

        if self.df is None:
            raise InvalidDatasetException("No dataframe was produced.")

        self.df = clean_dataset(self.df)
        self._validate_dataset()

        self.granularity_map = get_granularity_map(self.df)

        sorted_unique_cols = sorted({str(column) for column in self.df.columns})

        return {
            "filename": self.filename,
            "dataframe": self.df,
            "columns_str": str(sorted_unique_cols),
            "dataset_id": get_dataset_id(self.df),
            "granularity_map": self.granularity_map,
        }

    @abstractmethod
    def _read_file(self, nrows=None):
        raise NotImplementedError

    def _validate_dataset(self):
        if self.df is None:
            raise InvalidDatasetException("No dataset was loaded.")

        rows, columns = self.df.shape

        if rows == 0 or columns == 0:
            raise InvalidDatasetException("The dataset is empty.")

        if rows > Config.MAX_DATAFRAME_ROWS:
            raise InvalidDatasetException(
                f"Dataset exceeds {Config.MAX_DATAFRAME_ROWS} rows."
            )

        if columns > Config.MAX_DATAFRAME_COLS:
            raise InvalidDatasetException(
                f"Dataset exceeds {Config.MAX_DATAFRAME_COLS} columns."
            )

        if not self._dataset_has_header():
            raise InvalidDatasetException(
                "The dataset does not appear to have a usable header."
            )

    def _dataset_has_header(self):
        if self.df is None or len(self.df.columns) == 0:
            return False

        column_names = [str(column).strip() for column in self.df.columns]

        if not any(column_names):
            return False

        # reject when every header was generated from a blank cell.
        if all(name.lower().startswith("unnamed:") for name in column_names):
            return False

        return True

    @staticmethod
    def _read_limit(nrows):
        if nrows is not None:
            return min(nrows, Config.MAX_DATAFRAME_ROWS + 1)

        return Config.MAX_DATAFRAME_ROWS + 1

    @staticmethod
    def _validate_size_early(df: pd.DataFrame):
        if len(df) > Config.MAX_DATAFRAME_ROWS:
            raise InvalidDatasetException(
                f"Dataset exceeds {Config.MAX_DATAFRAME_ROWS} rows."
            )

        if len(df.columns) > Config.MAX_DATAFRAME_COLS:
            raise InvalidDatasetException(
                f"Dataset exceeds {Config.MAX_DATAFRAME_COLS} columns."
            )


def infer_simple_header_row(
    sample: pd.DataFrame,
    minimum_populated_cells=2,
    minimum_populated_ratio=0.5,
):
    if sample.empty:
        raise InvalidDatasetException("The file does not contain data.")

    populated_counts = sample.apply(
        lambda row: int(row.map(_has_value).sum()),
        axis=1,
    )

    non_empty_counts = populated_counts[populated_counts > 0]

    if non_empty_counts.empty:
        raise InvalidDatasetException("The file does not contain data.")

    # infer the approximate width of the actual table.
    apparent_width = int(non_empty_counts.quantile(0.9))

    minimum_required = max(
        minimum_populated_cells,
        math.ceil(apparent_width * minimum_populated_ratio),
    )

    for row_index in range(len(sample)):
        populated_count = int(populated_counts.iloc[row_index])

        if populated_count < minimum_required:
            continue

        next_row_index = _find_next_non_empty_row(
            sample,
            start=row_index + 1,
        )

        if next_row_index is None:
            continue

        return row_index

    return None


def _find_next_non_empty_row(
    df: pd.DataFrame,
    start,
):
    for row_index in range(start, len(df)):
        if df.iloc[row_index].map(_has_value).any():
            return row_index

    return None


def _has_value(value):
    if value is None:
        return False

    if isinstance(value, str):
        return bool(value.strip())

    try:
        return not bool(pd.isna(value))
    except (TypeError, ValueError):
        return True


def get_row_count_csv(upload_file: UploadFile):
    file = csv.reader(upload_file.file.read())
    row_count = sum(1 for _ in file)
    return row_count


def get_column_names_csv(upload_file: UploadFile):
    try:
        reader = csv.reader(io.BytesIO(upload_file.file.read()))
        try:
            headers = next(reader)
            return headers
        except StopIteration:
            return "this file has no headers"
    except Exception as e:
        return f"an error occured {e}"


class CsvReader(FileReader):
    def _read_file(self, nrows=None):
        try:
            encoding = self._detect_encoding()
            separator = self._detect_separator(encoding)

            sample = pd.read_csv(
                io.BytesIO(self.file_content),
                encoding=encoding,
                sep=separator,
                header=None,
                nrows=Config.HEADER_SCAN_ROWS,
                engine="python",
                dtype=object,
                skip_blank_lines=False,
            )

            if sample.shape[1] > Config.MAX_DATAFRAME_COLS:
                raise InvalidDatasetException(
                    f"Dataset exceeds {Config.MAX_DATAFRAME_COLS:,} columns."
                )

            header_row = infer_simple_header_row(sample)

            self.df = pd.read_csv(
                io.BytesIO(self.file_content),
                encoding=encoding,
                sep=separator,
                header=header_row,
                nrows=self._read_limit(nrows),
                engine="python",
                skip_blank_lines=True,
            )

            self._validate_size_early(self.df)

            return self.df

        except InvalidDatasetException:
            raise
        except (
            UnicodeDecodeError,
            pd.errors.ParserError,
            pd.errors.EmptyDataError,
            csv.Error,
            ValueError,
        ):
            raise FileReadException("Could not read CSV file")

    def _detect_encoding(self):
        sample = self.file_content[:100_000]

        for encoding in (
            "utf-8-sig",
            "utf-8",
            "cp1252",
            "latin-1",
        ):
            try:
                sample.decode(encoding)
                return encoding
            except UnicodeDecodeError:
                continue

        return "latin-1"

    def _detect_separator(self, encoding):
        text = self.file_content[:100_000].decode(
            encoding,
            errors="replace",
        )

        lines = [line for line in text.splitlines() if line.strip()]

        if not lines:
            raise InvalidDatasetException("The CSV contains no non-empty lines.")

        sample_text = "\n".join(lines[:20])

        try:
            dialect = csv.Sniffer().sniff(
                sample_text,
                delimiters=Config.SUPPORTED_DELIMITERS,
            )
            return dialect.delimiter
        except csv.Error:
            return self._fallback_separator(lines)

    @staticmethod
    def _fallback_separator(lines):
        """
        Select the delimiter with the most consistent field count.
        """
        candidates = [",", ";", "\t", "|"]
        best_separator = ","
        best_score = (-1, -1)

        for separator in candidates:
            counts = [len(line.split(separator)) for line in lines[:20]]

            multi_column_counts = [count for count in counts if count > 1]

            if not multi_column_counts:
                continue

            most_common_count = max(
                set(multi_column_counts),
                key=multi_column_counts.count,
            )

            consistency = multi_column_counts.count(most_common_count)
            field_count = most_common_count

            score = (consistency, field_count)

            if score > best_score:
                best_score = score
                best_separator = separator

        return best_separator


class XlsxReader(FileReader):
    def _read_file(self, nrows=None):
        try:
            selected_sheet_name = 0  # currently only supports reading the first sheet

            sample = pd.read_excel(
                io.BytesIO(self.file_content),
                sheet_name=selected_sheet_name,
                header=None,
                nrows=Config.HEADER_SCAN_ROWS,
                engine="openpyxl",
                dtype=object,
            )

            if sample.shape[1] > Config.MAX_DATAFRAME_COLS:
                raise InvalidDatasetException(
                    f"Dataset exceeds {Config.MAX_DATAFRAME_COLS} columns."
                )

            header_row = infer_simple_header_row(sample)

            self.df = pd.read_excel(
                io.BytesIO(self.file_content),
                sheet_name=selected_sheet_name,
                header=header_row,
                nrows=self._read_limit(nrows),
                engine="openpyxl",
            )

            self._validate_size_early(self.df)

            return self.df

        except InvalidDatasetException:
            raise
        except (
            ValueError,
            KeyError,
            OSError,
        ):
            raise FileReadException("could not read xlsx file")


def get_dataset_snippet(df: pd.DataFrame):
    return df.iloc[:5].to_csv(index=False)


def get_request_id_saved_dataset_dir(request_id, run_type):
    filename_dct = {
        TaskProcessingRunType.first_run_after_request.value: "original_dataset.parquet",
        TaskProcessingRunType.modified_tasks_execution.value: "original_dataset.parquet",
        TaskProcessingRunType.additional_analyses_request.value: "original_dataset.parquet",
        TaskProcessingRunType.modified_tasks_execution_with_new_dataset.value: "new_dataset.parquet",
    }
    filename = filename_dct[run_type]

    save_path = f"{Config.DATASET_SAVE_PATH}/{request_id}"
    file_dir = f"{save_path}/{filename}"

    return file_dir


def save_dataset_req_id(request_id, dataframe: pd.DataFrame, run_type):
    save_path = f"{Config.DATASET_SAVE_PATH}/{request_id}"
    if not os.path.exists(save_path):
        os.makedirs(save_path)

    file_dir = get_request_id_saved_dataset_dir(request_id, run_type)

    dataframe.to_parquet(file_dir, index=False)

    return file_dir
