from cranial.listeners.{{cookiecutter.listner}} import Listener
import {{cookiecutter.module}}
import {{cookiecutter.serde}} as serde

listener = Listener()
while True:
    data = listener.recv() # type: bytes
    data = serde.loads(data) # type: dict
    resp =  {{cookiecutter.module}}.{{cookiecutter.function}}(data)
    resp = serde.dumps(resp).encode('utf-8')
    listener.resp(resp)