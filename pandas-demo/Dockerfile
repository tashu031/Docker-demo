FROM python:slim

WORKDIR /app

COPY requirement.txt .

RUN pip install -r requirement.txt 

COPY app.py .

CMD ["python", "app.py"]