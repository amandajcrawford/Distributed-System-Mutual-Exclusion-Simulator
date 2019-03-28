class RequestQueue:
    READ_OP = 1
    WRITE_OP = 2

    def __init__(self):
        self.requests = []
        self.queueSize = 0
        self.last_msg_time = 0

    def add(self, msg):
        if self.queueSize == 0:
            self.requests.append(msg)
            self.queueSize += 1
        else:
            msg_time = msg['time']
            msg_op = msg['op']
            for i in range(self.queueSize-1, -1, -1):
                item = self.requests[i]
                item_time = item['time']
                if item_time < msg_time:                 # compare timestamps
                    self.requests.insert(i, msg)
                    self.queueSize += 1
                    break
                elif item_time == msg_time:              # if timestamps equal, compare id
                    msg_cid = msg['cid']
                    item_cid = item['cid']
                    item_op = item['op']
                    if msg_op == self.READ_OP and item_op == self.WRITE_OP:
                        self.requests.insert(i-1, msg)
                        self.queueSize += 1
                    else:
                        if item_cid < msg_cid:             # lesser id wins
                            self.requests.insert(i, msg)
                            self.queueSize += 1
                            break
                        else:
                            self.requests.insert(i-1, msg)
                            self.queueSize += 1
                            break

    def get_first(self):
        if self.queueSize > 0:
            return self.requests[0]
        return None

    def popRequest(self):
        self.queueSize = self.queueSize - 1
        return self.requests.pop(0)
