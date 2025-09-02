import pandas as pd
from pandasai import SmartDataframe
from pandasai.llm.openai import OpenAI
import os, json

from dotenv import load_dotenv


def summary_data(arguments):
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")
    data_path, prompt = arguments["data_path"], arguments["prompt"]

    data = pd.read_excel(data_path)

    llm = OpenAI(api_token=api_key)
    pandasai = SmartDataframe(data, config={"llm": llm, "verbose": True})

    ai_result = pandasai.chat(f"{prompt}, return all columns and rows")

    try:
        result = pd.DataFrame(ai_result).to_string()
        return json.dumps(result)

    except:
        return json.dumps(str(ai_result))


def summary_data_description():
    description = {
        "name": "summary_data",
        "description": "PandasAI가 입력된 프롬프트를 기준으로 데이터를 반환합니다.",
        "parameters": {
            "type": "object",
            "properties": {
                "data_path": {
                    "type": "string",
                    "description": "분석이 필요한 파일 경로",
                },
                "prompt": {
                    "type": "string",
                    "description": "분석할 내용을 입력합니다. 영어로 입력해야 하며, 분석 기간을 명시합니다.",
                },
            },
            "required": ["data_path", "prompt"],
        },
    }
    return description


if __name__ == "__main__":

    arguments = {
        "data_path": "C:/Users/blueorange/Desktop/캐롯_키워드리포트.xlsx",
        "prompt": """6월 노출 수 합은?""",
    }

    result = summary_data(arguments)
    print("==========RESULT==========\n", json.loads(result))
