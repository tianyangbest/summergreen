FROM python:3.7.9-buster
COPY requirements.txt /
RUN pip install -i https://mirrors.aliyun.com/pypi/simple -r requirements.txt
COPY summergreen /summergreen

EXPOSE 80
CMD ["uvicorn", "summergreen.app.main:app", "--host", "0.0.0.0", "--port", "80"]