class Ace:
    def __init__(self, name, read, write):
        self.name = name
        self.read = read
        self.write = write

    def data(self):
        data = dict()
        # Parse REST API does not like an attribute with a value of false.
        # Only add attribute with a true value to the dictionary
        if self.read:
            data['read'] = self.read
        if self.write:
            data['write'] = self.write
        return data


class Acl:
    def __init__(self, ace=None):
        if ace is None:
            self.ace_list = []
        else:
            self.ace_list = ace

    def add(self, name, read=False, write=False):
        self.ace_list.append(Ace(name, read, write))

    def data(self):
        data = dict()
        for ace in self.ace_list:
            data[ace.name] = ace.data()
        return data
