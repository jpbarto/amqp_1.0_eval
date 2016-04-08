from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container

class Handler (MessagingHandler):
    def __init__ (self, url, target):
        super (Handler, self).__init__ ()
        self.url = url
        self.target = target

    def on_start (self, event):
        conn = event.container.connect (self.url)
        event.container.create_receiver (conn, self.target)

    def on_message (self, event):
        print ("Got message: {0}".format (event.message.body))
        event.receiver.close ()
        event.connection.close ()

try:
    Container (Handler ("localhost:5672", "queue://exam.ple.que.ue")).run ()
except KeyboardInterrupt:
    pass
