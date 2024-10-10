from sqlalchemy import Row


class MutableRow:
    def __init__(self, row: Row):
        new_data = list(row._data)
        self._mutable_data = new_data

    def __getitem__(self, index):
        return self._mutable_data[index]

    def __setitem__(self, index, value):
        self._mutable_data[index] = value

    @property
    def _data(self):
        return tuple(self._mutable_data)

    def __len__(self):
        return len(self._mutable_data)