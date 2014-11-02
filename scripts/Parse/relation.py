class Reference:
    def __init__(self, obj):
        self.obj = obj
        self.addition = True
        self.updated = False

    def data(self):
        data = dict()
        data['__type'] = 'Pointer'
        data['className'] = '_' + self.obj.__class__.__name__
        data['objectId'] = self.obj.id
        return data


class Relation:
    def __init__(self, class_=None, obj_list=None):
        self.class_ = class_
        self.obj_list = []
        if obj_list is not None:
            for obj in obj_list:
                self.obj_list.append(Reference(obj))

    def _get_obj_list(self, addition):
        obj_list = []
        for obj in self.obj_list:
            if obj.updated:
                continue
            if obj.addition == addition:
                obj_list.append(obj)
        return obj_list

    def has_update(self, addition):
        return len(self._get_obj_list(addition)) > 0

    def add(self, obj):
        if self.class_ is not None:
            assert obj.__class__ == self.class_
        self.obj_list.append(Reference(obj))

    def data(self, create=True, addition=True):
        if not create:  # this is an update
            obj_list = self._get_obj_list(addition)
        else:
            obj_list = self.obj_list
        if len(obj_list) == 0:
            return dict()
        data = dict()
        if create or addition:
            data['__op'] = 'AddRelation'
        else:
            data['__op'] = 'RemoveRelation'
        data['objects'] = []
        for obj in obj_list:
            data['objects'].append(obj.data())
        return data

    def update(self, addition):
        obj_list = self._get_obj_list(addition)
        for obj in obj_list:
            obj.updated = False
            if not obj.addition:
                self.obj_list.remove(obj)
