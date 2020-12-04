import pika
import uuid


class WebRpcClient(object):

    # 连接rabbitmq，声明了一个callback_queue准备接结果
    def __init__(self):
        credentials = pika.PlainCredentials('xuan', '123456')  # mq用户名和密码
        # 虚拟队列需要指定参数 virtual_host，如果是默认的可以不填。
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='210.30.97.163', port=5672, virtual_host='/', credentials=credentials))
        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue = 'rpc_queue',
                                       durable=True)
        self.callback_queue = result.method.queue

        # 准备接受命令结果，当收到消息调用on_response
        # 收到这个消息之后，调用callback函数，还要做1、把response改为非none，2、检查唯一标识符能不能对上。
        self.channel.basic_consume(self.callback_queue,
                                   self.on_response,
                                   auto_ack=True)

    def on_response(self, ch, method, props, body): #props端返回的
        """callback函数"""
        if self.corr_id == props.correlation_id: # 收到消息后检查唯一标识符
            self.response = body #给response赋值，body(计算的值)，response就不为none了

    def call(self, n):
        self.response = None
        self.corr_id = str(uuid.uuid1()) #唯一表示
        self.channel.basic_publish(exchange='',
                                   routing_key='rpc_queue', #这里是一对一的发
                                   body=str(n),
                                   properties=pika.BasicProperties(
                                       correlation_id=self.corr_id,  # 唯一标识符也发过去
                                       reply_to=self.callback_queue, #结果定义发送到这里
                                   ))
        while self.response is None: # 在什么地方吧response改成不为none？
            self.connection.process_data_events() # 检查队列里有没有新消息，但是不会阻塞
        return self.response


web_rpc = WebRpcClient()

print(" [x] Requesting()")
response = web_rpc.call(111111)
print(" [.] Got %r" % response)