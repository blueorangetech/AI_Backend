import openai, json, aiohttp, asyncio
from tools.math_tool import mathematics, mathematics_description
from tools.ai_analyst import summary_data, summary_data_description
from tools.group_data import group_data

def interim_report(msg, agent=None):
    funtcions = [
        # mathematics_description(),
        # summary_data_description()
    ]

    message = [msg] if agent is None else [agent, msg]

    while True:
        response = openai.ChatCompletion.create(
            model='gpt-4o-mini',
            messages = message,
            temperature = 0.7,
            # functions = funtcions,
            # function_call="auto"
            )
        
        response_message = response['choices'][0]['message']
        
        if response_message.get('function_call'):
            available_function = {
                # "mathematics" : mathematics,
                # "summary_data": summary_data,
            }

            function_name = response_message['function_call']['name']

            function_to_call = available_function[function_name]
            function_args = json.loads(response_message['function_call']['arguments'])
            
            function_response = function_to_call(
                arguments = function_args
            )
            
            message.append(response_message)
            message.append({
                'role': 'function',
                'name': function_name,
                'content' : function_response
            })

        else:
            return response_message['content']