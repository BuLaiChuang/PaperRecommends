import pika
import MySQLdb

credentials = pika.PlainCredentials('xuan', '123456')  # mq用户名和密码
# 虚拟队列需要指定参数 virtual_host，如果是默认的可以不填。
connection = pika.BlockingConnection(pika.ConnectionParameters(host='210.30.97.163', port=5672, virtual_host='/', credentials=credentials))
channel = connection.channel()

result = channel.queue_declare(queue = 'rpc_queue',durable=True)

# 调用盛夏代码查询相关的论文
def PaperRecom(n):
    # 打开数据库连接
    db = MySQLdb.connect(host="210.30.97.163", user="root", passwd="123456", db="rpcmysql", charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    a = str(n)
    # SQL 查询语句
    sql = "SELECT name FROM student where id = '"+a+"'"

    # 执行SQL语句
    cursor.execute(sql)
    # 获取所有记录列表
    results = cursor.fetchall()
    # 关闭数据库连接
    cursor.close()
    db.close()

    print("学号 姓名")
    for it in results:
        for i in range(len(it)):
            print(it[i])
            return it[i]


def on_request(ch, method, props, body):
    n = int(body)
    print(" [.] shengxia(%s)" % n)
    response = PaperRecom(n)
    ch.basic_publish(exchange='',
                     routing_key=props.reply_to, # 从props中取出客户端放的reply_to
                     properties=pika.BasicProperties(correlation_id= \
                                                         props.correlation_id),# 客户端发的唯一标识符
                     body=response)
    ch.basic_ack(delivery_tag=method.delivery_tag) # 执行完

# 一次只处理一个任务，处理完后再拿
channel.basic_qos(prefetch_count=1)
channel.basic_consume('rpc_queue',on_request)

print(" [x] Awaiting RPC requests")
channel.start_consuming()