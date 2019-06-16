from neo.VM.InteropService import Array, Map, Struct


def getItemCount(item):
    #if type(item) == Array or type(item) == Struct:
    if isinstance(item, Array):
        return sum(getItemCount(subitem) for subitem in item.GetArray())
    if type(item) == Map:
        return sum(getItemCount(subitem) for subitem in item.Values)
    if type(item) == list:
        return sum(getItemCount(subitem) for subitem in item)
    else:
        return 1


class RandomAccessStack:

    def __init__(self, name='Stack'):
        self._list = []
        self._size = 0
        self._total_size = 0
        self._name = name
        self._max_size = 2048

    @property
    def Count(self):
        return self._size

    @property
    def TotalCount(self):
        return self._total_size

    @property
    def Items(self):
        return self._list

    def Clear(self):
        self._list = []
        self._size = 0
        self._total_size = 0

    def CopyTo(self, stack, count=-1):
        if count == 0:
            return
        if count == -1:
            stack._list.extend(self._list)
            stack._size += self._size
            stack._total_size += self._getItemCount(self._list)
            if stack._total_size > self._max_size:
                raise InvalidStackSize
        else:
            # only add the last ``count`` elements of self._list
            skip_count = self._size - count
            skipped_list = self._list[skip_count:]
            stack._list.extend(skipped_list)
            stack._size += count
            stack._total_size += self._getItemCount(skipped_list)
            if stack._total_size > self._max_size:
                raise InvalidStackSize

    def GetEnumerator(self):
        return enumerate(self._list)

    def Insert(self, index, item):
        index = int(index)

        if index > self._size:
            raise Exception("Invalid list operation")

        self._list.insert(self._size - index, item)
        self._size += 1
        self._total_size += self._getItemCount(item)
        if self._total_size > self._max_size:
            raise InvalidStackSize

    # @TODO can be optimized
    def Peek(self, index: int = 0):
        if index == 0:
            return self._list[-1]

        if index >= self._size:
            raise Exception("Invalid list operation")

        if index < 0:
            index += self._size

        if index < 0:
            raise Exception("Invalid list operation")

        return self._list[self._size - index - 1]

    def Pop(self):
        return self.Remove(0)

    def PushT(self, item):
        # to prevent circular import
        from neo.VM.InteropService import StackItem
        if not type(item) is StackItem and not issubclass(type(item), StackItem):
            item = StackItem.New(item)

        self._list.append(item)
        self._size += 1
        self._total_size += self._getItemCount(item)
        if self._total_size > self._max_size:
            raise InvalidStackSize

    # @TODO can be optimized
    def Remove(self, index: int):

        if index == 0:
            item = self._list.pop(-1)
            self._size -= 1
            self._total_size -= self._getItemCount(item)
            return item

        if index >= self._size:
            raise Exception("Invalid list operation")
        if index < 0:
            index += self._size
        if index < 0:
            raise Exception("Invalid list operation")

        item = self._list.pop(self._size - 1 - index)
        self._size -= 1
        self._total_size -= self._getItemCount(item)

        return item

    def Set(self, index, item):
        from neo.VM.InteropService import StackItem
        index = int(index)

        if index >= self._size:
            raise Exception("Invalid list operation")
        if index < 0:
            index += self._size
        if index < 0:
            raise Exception("Invalid list operation")

        if not type(item) is StackItem and not issubclass(type(item), StackItem):
            item = StackItem.New(item)

        old_item = self._list[self._size - index - 1]
        self._total_size -= self._getItemCount(old_item)
        self._list[self._size - index - 1] = item
        self._total_size += self._getItemCount(item)
        if self._total_size > self._max_size:
            raise InvalidStackSize

    def _getItemCount(self, item):
        #real_count = 0
        #try:
        count = getItemCount(item)
        real_count = count if count > 0 else 1
        #except RecursionError:
        #    # possible but very unlikely, so not handled yet
        #    # if handled, process collection in chunks (generator)
        #    pass
        return real_count


class InvalidStackSize(Exception):
    pass

