FROM python:3.9-slim

# Copy mã nguồn vào container
COPY . /app
WORKDIR /app

# Cài đặt các dependencies
RUN pip install -r requirements.txt

# Chạy ứng dụng của bạn (Consumer)
CMD ["python", "consumer.py"]
