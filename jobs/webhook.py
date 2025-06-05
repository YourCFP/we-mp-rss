from core.models.message_task import MessageTask
def web_hook(data:dict,task:MessageTask=None,count:int=0):
    print(data)
