from pydantic import BaseModel


class MsgDetail(BaseModel):
    id: int
    MsgSvrID: str
    type_name: str
    is_sender: int
    talker: str
    room_name: str
    msg: str
    src: str
    extra: dict
    CreateTime: str
    Sequence: int
