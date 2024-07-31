import json

def mathematics(expr):
    """ 수학 공식을 계산해주는 함수"""
    exp = expr['expr']
    try:
        result = eval(exp)
        return json.dumps(result)
    
    except Exception as e:
        raise ValueError(f"Invalid expression: {expr}") from e
    

def mathematics_description():
    description = {
            "name": "mathematics",
            "description": "수식을 계산합니다.",
            "parameters" : {
                "type" : "object",
                "properties" :{
                    "expr": {
                        "type": "string",
                        "description" : "계산할 수학 공식"
                    }
                },
                "required" : ["expr"]
            }
        }
    return description