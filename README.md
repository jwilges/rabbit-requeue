# rabbit-requeue
*a primitive message requeuer for RabbitMQ*

## Initialization
```bash
python -m venv --prompt rabbit-requeue .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage
Update `configuration.yml` to match your desired source/sink queues, then:
```bash
python requeue.py
```

Refer to the `-h` argument for full usage information.