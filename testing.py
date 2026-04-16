from pydantic import BaseModel 
class User(BaseModel):
    
    name: str
    age: int
user = User(name ='Sam', age = 25)
dumped_user = user.model_dump()
print(dumped_user)
print(type(dumped_user))

json_user = user.model_dump_json()
print(json_user)