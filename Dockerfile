FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV TASKGRID_DB=/data/taskgrid.db
ENV TASKGRID_MANAGER_LOG=/data/manager.log
ENV PYTHONUNBUFFERED=1
RUN mkdir -p /data /logs
EXPOSE 8000
EXPOSE 9201
CMD ["uvicorn", "taskgrid.app:app", "--host", "0.0.0.0", "--port", "8000"]
