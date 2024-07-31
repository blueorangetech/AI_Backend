import openai, json
from tools.math_tool import mathematics, mathematics_description
from tools.group_data import group_data, group_data_description

def interim_report(idx, message, chunk):
    print(idx, '번 데이터 처리 중')

    funtcions = [
        mathematics_description(),
        # group_data_description(),
    ]

    while True:
        
        response = openai.ChatCompletion.create(
            model='gpt-4o-mini',
            messages = message,
            temperature = 0.75,
            functions = funtcions,
            function_call="auto"
            )
        
        response_message = response['choices'][0]['message']
        print(response_message)
        if response_message.get('function_call'):
            available_function = {
                "mathematics" : mathematics,
                "group_data" : group_data,
            }

            function_name = response_message['function_call']['name']

            if function_name not in available_function:
                return "사용할 수 없는 함수입니다."
            
            function_to_call = available_function[function_name]
            function_args = json.loads(response_message['function_call']['arguments'])
            
            function_response = function_to_call(
                # data = chunk,
                # fields = function_args['fields'],
                # sum_fields = function_args['sum_fields']
                expr = function_args
            )
            
            message.append(response_message)
            message.append({
                'role': 'function',
                'name': function_name,
                'content' : function_response
            })

        else:
            return response_message['content']


def data_summary(prompt):
    response = openai.ChatCompletion.create(
        model='gpt-4o-mini',
        messages=[
            {
                "role": "system",
                "content": [
                    {"type": "text",
                     "text": """
                            당신은 10년 차 데이터 분석가 리더다.
                            입력 받은 개별 리포트를 합산하여
                            광고 매체, 디바이스 별 노출, 클릭, 광고비, 전환 데이터를 제시한다

                            데이터를 제시할 때, CTR 및 CVR 등의 지표 데이터를 함께 제시하며
                            데이터를 기간 단위로 비교가 가능한 경우
                            비교 기간 대비 변동률을 함께 제시한다.
                            """
                            }]
                            },
                            {
                                "role": "user",
                                "content": prompt
                                }])
    
    return response['choices'][0]['message']['content']