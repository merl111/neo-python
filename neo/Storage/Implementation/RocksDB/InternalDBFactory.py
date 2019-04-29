from neo.Storage.Implementation.AbstractDBImplementation import AbstractDBImplementation


def internalDBFactory(classPrefix):

    # import what's needed
    import neo.Storage.Implementation.RocksDB.RocksDBClassMethods as functions

    methods = [x for x in dir(functions) if not x.startswith('__')]

    # build attributes dict
    attributes = {methods[i]: getattr(
        functions, methods[i]) for i in range(0, len(methods))}

    # add __init__ method
    if classPrefix == 'Snapshot':
        attributes['__init__'] = attributes.pop(functions._snap_init_method)
    elif classPrefix == 'Prefixed':
        attributes['__init__'] = attributes.pop(functions._prefix_init_method)

    return type(
        classPrefix.title() + 'DBImpl',
        (AbstractDBImplementation,),
        attributes)
