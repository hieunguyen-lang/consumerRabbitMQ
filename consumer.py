import pika
import json
import pymysql
from pymysql import Error

class RabbitMQConsumer:
    def __init__(self, rabbitmq_host='rabbitmq', rabbitmq_queue='data_crawled', mysql_host='mysql', mysql_user='hieunk', mysql_password='123456', mysql_db='crawl_data_express',mysql_port=3306, rambbitmq_port=5672):
        # Kết nối RabbitMQ
        credentials = pika.PlainCredentials('admin', 'admin')
        self.rabbitmq_host = rabbitmq_host
        self.rabbitmq_port = rambbitmq_port
        self.rabbitmq_queue = rabbitmq_queue
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host,port=rambbitmq_port,credentials=credentials,virtual_host='/'))
        self.channel = self.connection.channel()

        # Kết nối MySQL
        self.mysql_host = mysql_host
        self.mysql_user = mysql_user
        self.mysql_password = mysql_password
        self.mysql_db = mysql_db
        self.mysql_port = mysql_port
        self.mysql_connection = self.connect_mysql()

        # Đảm bảo rằng queue tồn tại
        self.channel.queue_declare(queue=self.rabbitmq_queue, durable=True)
        
    def connect_mysql(self):
        try:
            # Kết nối đến MySQL
            connection = pymysql.connect(
                host=self.mysql_host,
                user=self.mysql_user,
                password=self.mysql_password,
                database=self.mysql_db,
                port=self.mysql_port,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            print("Kết nối MySQL thành công")
            return connection
        except Error as e:
            print(f"Lỗi kết nối MySQL: {e}")
            return None

    def insert_to_mysql(self, item):
        if self.mysql_connection is not None:
            cursor = self.mysql_connection.cursor()
            query = """INSERT INTO articles (title, description, url, author, published_date, content, tags, image_url) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
            data = (item['title'], item['description'], item['url'], item['author'], item['published_date'], item['content'], item['tags'], item['image_url'])
            try:
                cursor.execute(query, data)
                self.mysql_connection.commit()
                print(f"Đã lưu bài viết '{item['title']}' vào MySQL")
            except Error as e:
                print(f"Lỗi khi lưu dữ liệu vào MySQL: {e}")

    def callback(self, ch, method, properties, body):
        #print(f"Đang xử lý message: {body}")
        try:
            # Chuyển body về JSON
            message = json.loads(body)

            # Lưu dữ liệu vào MySQL
            self.insert_to_mysql(message)

            # Xác nhận rằng message đã được xử lý thành công
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"Lỗi xử lý message: {e}")

    def start_consuming(self):
        print(f"Đang lắng nghe queue '{self.rabbitmq_queue}'...")
        self.channel.basic_consume(queue=self.rabbitmq_queue, on_message_callback=self.callback)
        self.channel.start_consuming()


if __name__ == "__main__":
    consumer = RabbitMQConsumer(rabbitmq_host='rabbitmq', rabbitmq_queue='data_crawled', mysql_host='mysql', mysql_user='hieunk', mysql_password='123456', mysql_db='crawl_data_express',mysql_port=3306, rambbitmq_port=5672)
    consumer.start_consuming()
