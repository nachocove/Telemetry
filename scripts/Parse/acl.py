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

    def __eq__(self, other):
        if not isinstance(other, Ace):
            return False
        return (self.name == other.name) and (self.read == other.read) and (self.write == other.write)

    def __ne__(self, other):
        return not (self == other)


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

    def parse(self, data):
        self.ace_list = []
        for (name, ace) in data.items():
            read = ace.get('read', False)
            write = ace.get('write', False)
            self.ace_list.append(Ace(name=name, read=read, write=write))

    def __eq__(self, other):
        if not isinstance(other, Acl):
            return False
        if len(self.ace_list) != len(other.ace_list):
            return False
        for n in range(len(self.ace_list)):
            if self.ace_list[n] != other.ace_list[n]:
                return False
        return True

    def __ne__(self, other):
        return not (self == other)