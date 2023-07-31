import json
from base64 import b64decode

def decode_jwt(jwt: str):
    header, body, _sig, *_rest = jwt.split('.')
    decoded_body = json.loads(b64decode(body.encode("utf-8") + b"====").decode("utf-8"))
    decoded_header = json.loads(b64decode(header.encode("utf-8") + b"====").decode("utf-8"))
    return decoded_header, decoded_body