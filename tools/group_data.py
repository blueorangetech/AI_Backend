import pandas as pd
import numpy as np
import traceback


def group_data(arguments):
    data, standrad, fields = (
        arguments["data"],
        arguments["standard"],
        arguments["fields"],
    )
    sum_fields, limit = arguments["sum_fields"], arguments["limit"]

    if limit is None:  # 기간 전체 데이터를 비교할 때
        filterd = data.groupby(fields + standrad).sum(numeric_only=True).reset_index()
        calc_fields = []
        diff_info = {}
        try:
            for field in sum_fields:
                if isinstance(field, list):
                    field_name, a, b, c = field
                    filterd[field_name] = (
                        filterd.apply(
                            lambda x: x[a] / x[b] if x[b] > 0 else None, axis=1
                        )
                        * c
                    )
                    calc_fields.append(field_name)

                else:
                    calc_fields.append(field)

            data_sorted = filterd.sort_values(
                by="기준", key=lambda x: x.str.contains("분석 기간"), ascending=True
            ).reset_index(drop=True)

            for field in calc_fields:
                diff_info[f"{field} - 증감"] = data_sorted[field].diff().iloc[-1]
                variable = data_sorted[field].pct_change() * 100
                diff_info[f"{field} - 증감%"] = round(variable.iloc[-1], 2)

            result = {"data": data_sorted, "variable": diff_info}
            return result

        except Exception as e:
            print(str(e))

    try:

        if isinstance(sum_fields, str):  # 노출 등 단일 지표를 구할 때
            col_name = sum_fields
            filterd = (
                data.groupby(fields + standrad)[sum_fields]
                .sum(numeric_only=True)
                .reset_index()
            )

        else:  # CPC 등 비율 지표를 구할 때
            col_name, target1, target2, c = sum_fields
            filterd = (
                data.groupby(fields + standrad)[[target1, target2]]
                .sum(numeric_only=True)
                .reset_index()
            )

            filterd[col_name] = (
                filterd.apply(
                    lambda x: x[target1] / x[target2] if x[target2] > 0 else None,
                    axis=1,
                )
                * c
            )
            filterd = filterd.drop(columns=[target1, target2])

        # 데이터를 비교 기간과 분석 기간으로 나눈 후 Merge
        standard_data = filterd.loc[filterd["기준"] == "분석 기간"]
        compare_data = filterd.loc[filterd["기준"] == "비교 기간"]

        merge_data = pd.merge(standard_data, compare_data, on=fields, how="left")
        merge_data.fillna(0, inplace=True)

        if isinstance(sum_fields, str):
            merge_data["증감 수치"] = (
                merge_data[f"{sum_fields}_x"] - merge_data[f"{sum_fields}_y"]
            )
            merge_data["증감률"] = (
                merge_data[f"{sum_fields}_x"] / merge_data[f"{sum_fields}_y"] - 1
            ) * 100

        else:
            merge_data["증감 수치"] = (
                merge_data[f"{sum_fields[0]}_x"] - merge_data[f"{sum_fields[0]}_y"]
            )
            merge_data["증감률"] = (
                merge_data[f"{sum_fields[0]}_x"] / merge_data[f"{sum_fields[0]}_y"] - 1
            ) * 100

        merge_data.sort_values(by="증감 수치", ascending=False, inplace=True)

        # 필드 이름 수정 및 정리
        if isinstance(sum_fields, str):
            merge_data.rename(
                columns={
                    f"{sum_fields}_x": "분석 기간",
                    f"{sum_fields}_y": "비교 기간",
                },
                inplace=True,
            )

        else:
            merge_data.rename(
                columns={
                    f"{sum_fields[0]}_x": "분석 기간",
                    f"{sum_fields[0]}_y": "비교 기간",
                },
                inplace=True,
            )

        merge_data.drop(columns=["기준_x", "기준_y"], inplace=True)

        head_data = merge_data[:limit]
        tail_data = merge_data[-limit:]

        concat_data = pd.concat([head_data, tail_data])

        if isinstance(sum_fields, str):
            total_data = {"지표": sum_fields, "data": concat_data}
            return total_data

        else:
            total_data = {"지표": sum_fields[0], "data": concat_data}
            return total_data

    except:
        traceback.print_exc()
