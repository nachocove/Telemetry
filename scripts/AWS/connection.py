from boto.dynamodb2.layer1 import DynamoDBConnection


class Connection(DynamoDBConnection):
    def __init__(self, **kwargs):
        self.params = kwargs
        DynamoDBConnection.__init__(self, **kwargs)

    def clone(self):
        return Connection(**self.params)
