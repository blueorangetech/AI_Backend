import json


def mathematics(arguments):
    """Python 코드를 실행하는 함수"""
    exp = arguments["expr"]
    try:
        result = eval(exp)
        return json.dumps(result)

    except Exception as e:
        return json.dumps({"error": f"Invalid expression: {exp}, error: {str(e)}"})


def mathematics_description():
    description = {
        "name": "mathematics",
        "description": "Python 코드를 동작시킵니다.",
        "parameters": {
            "type": "object",
            "properties": {
                "expr": {"type": "string", "description": "Input Python code"}
            },
            "required": ["expr"],
        },
    }
    return description
