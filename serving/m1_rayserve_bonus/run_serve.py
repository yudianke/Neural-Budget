import time
import ray
from ray import serve
from serve_app import deployment

ray.init(address="auto", ignore_reinit_error=True)

serve.start(
    detached=False,
    http_options={
        "host": "0.0.0.0",
        "port": 8000,
    },
)

serve.run(deployment, name="default")

print("Ray Serve app is running on 0.0.0.0:8000")

while True:
    time.sleep(3600)
