import pandas as pd


def chunk_dict(data, size):
    """딕셔너리를 청크로 분할합니다."""
    keys = list(data.keys())

    chunks = []
    data_size = len(pd.DataFrame(data))
    chunk_size = data_size // size

    for i in range(0, data_size, chunk_size):
        chunk = {k: data[k][i : i + chunk_size] for k in keys}
        chunks.append(chunk)

    return chunks


def dataframe_chunk(data, size):
    chunks = []
    chunk_size = len(data) // size

    for i in range(0, len(data), chunk_size):
        chunks.append(data[i : i + chunk_size])

    return chunks


def timestamp_to_str(data):
    """엑셀 날짜 형식을 문자열로 반환힙니다."""
    columns = data.columns

    for column in columns:
        if data[column].dtypes == "datetime64[ns]":
            data[column] = data[column].astype(str)

    return data.to_dict(orient="list")
