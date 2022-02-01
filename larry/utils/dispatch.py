from weakref import WeakKeyDictionary
from abc import get_cache_token
from functools import update_wrapper
from types import MappingProxyType, ModuleType
from functools import singledispatch


def larrydispatch(func):
    module_name_registry = {}
    callable_name_registry = {}
    type_name_registry = {}
    class_name_registry = {}
    eq_registry = {}
    sd = singledispatch(func)
    dispatch_cache = WeakKeyDictionary()
    def ns(): pass
    ns.cache_token = None

    def dispatch(value):
        if ns.cache_token is not None:
            current_token = get_cache_token()
            if ns.cache_token != current_token:
                dispatch_cache.clear()
                ns.cache_token = current_token
        try:
            impl = dispatch_cache[value]
        except KeyError:
            if isinstance(value, ModuleType) and value.__name__ in module_name_registry:
                impl = module_name_registry[value.__name__]
            elif callable(value) and value.__name__ in callable_name_registry:
                impl = callable_name_registry[value.__name__]
            elif value in eq_registry:
                impl = eq_registry[value]
            elif value.__class__.__name__ in class_name_registry:
                impl = class_name_registry[value.__class__.__name__]
            elif value.__name__ in type_name_registry:
                impl = callable_name_registry[value.__name__]
            else:
                impl = sd.dispatch(value.__class__)
            dispatch_cache[value] = impl
        return impl

    def register_module_name(name: str, func=None):
        if func is None:
            return lambda f: register_module_name(name, f)
        if not isinstance(name, str):
            raise ValueError("Module name must be a str value")
        module_name_registry[name] = func
        return func

    def register_type_name(name, func=None):
        if func is None:
            return lambda f: register_type_name(name, f)
        if not isinstance(name, str):
            raise ValueError("Type name must be a str value")
        type_name_registry[name] = func
        return func

    def register_callable_name(name, func=None):
        if func is None:
            return lambda f: register_callable_name(name, f)
        if not isinstance(name, str):
            raise ValueError("Callable name must be a str value")
        callable_name_registry[name] = func
        return func

    def register_class_name(name, func=None):
        if func is None:
            return lambda f: register_class_name(name, f)
        if not isinstance(name, str):
            raise ValueError("Class name must be a str value")
        class_name_registry[name] = func
        return func

    def register_eq(cls, func=None):
        if func is None:
            return lambda f: register_eq(cls, f)
        eq_registry[cls] = func
        return func

    def register(cls, func=None):
        if func is None:
            return lambda f: register(cls, f)
        sd.register(cls, func)
        return func

    def wrapper(*args, **kw):
        if not args:
            raise TypeError('{0} requires at least '
                            '1 positional argument'.format(funcname))
        return dispatch(args[0])(*args, **kw)

    funcname = getattr(func, '__name__', 'singledispatch function')
    wrapper.register_module_name = register_module_name
    wrapper.register_type_name = register_type_name
    wrapper.register_callable_name = register_callable_name
    wrapper.register_class_name = register_class_name
    wrapper.register_eq = register_eq
    wrapper.register = register
    wrapper.dispatch = dispatch
    wrapper.module_name_registry = MappingProxyType(module_name_registry)
    wrapper.callable_name_registry = MappingProxyType(callable_name_registry)
    wrapper.type_name_registry = MappingProxyType(type_name_registry)
    wrapper.class_name_registry = MappingProxyType(class_name_registry)
    wrapper.eq_registry = MappingProxyType(eq_registry)
    wrapper.registry = sd.registry
    wrapper._clear_cache = dispatch_cache.clear
    update_wrapper(wrapper, func)
    return wrapper

"""
def currydispatch(func):
    module_name_registry = {}
    callable_name_registry = {}
    type_name_registry = {}
    class_name_registry = {}
    eq_registry = {}
    sd = singledispatch(func)
    dispatch_cache = WeakKeyDictionary()
    def ns(): pass
    ns.cache_token = None
"""