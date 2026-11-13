FROM python:3.12-alpine
# Standard library only: the script talks to the API server with urllib and a
# service-account token, no kubectl / curl / jq inside the image
WORKDIR /app
COPY balancer.py /app/balancer.py
USER 65534:65534
ENTRYPOINT ["python3", "/app/balancer.py"]
