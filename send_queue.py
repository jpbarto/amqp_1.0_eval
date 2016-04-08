from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container

class Sender (MessagingHandler):
    def __init__ (self, url, target):
        super (Sender, self).__init__ ()
        self.url = url
        self.target = target
        self.sent = 0
        self.confirmed = 0
        self.send_limit = 10

    def on_start (self, event):
        # conn = event.container.connect (self.url)
        event.container.create_sender ("{0}/{1}".format (self.url, self.target))

    def on_sendable (self, event):
        while event.sender.credit and self.sent < self.send_limit:
            mid = self.sent + 1
            msg = Message (id = mid, body = {'sequence': mid})
            event.sender.send (msg)
            self.sent += 1
            print ("Sent {0}".format (msg))

    def on_accepted (self, event):
        self.confirmed += 1
        if self.confirmed == self.send_limit:
            print ("All messages confirmed received")
            event.connection.close ()

    def on_disconnected (self, event):
        print ("Shutdown complete")

try:
    Container (Sender ("localhost:5672", "queue://exam.ple.que.ue")).run ()
except KeyboardInterrupt:
    pass
