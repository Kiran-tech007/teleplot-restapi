FROM python:3.11-slim
RUN pip install --upgrade pip
RUN pip install --upgrade setuptools wheel
CMD ["python", "app.py"]

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY .env /.env
COPY app.py /app.py