from collections.abc import Hashable
from weakref import WeakKeyDictionary
from abc import get_cache_token
from functools import update_wrapper
from types import MappingProxyType, ModuleType
from functools import singledispatch
import inspect


def larrydispatch(func):
    """
    An extension of the singledispatch decorator that allows for registering implementations using additional
    types and values. This supports the `read_as` and `write_as` functionality implemented in s3 and lmbda by
    allowing for support of types such as PIL.Image and np.ndarray without the need to install the source package
    or stubs.

    Supports registering by:
    * module name ("PIL.Image")
    * callable name ("imwrite")
    * type name ("ndarray")
    * class name ("PngImageFile")
    * equivalence (str, dict)
    * value type (basic single dispatch)
    """
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
        except (TypeError, KeyError):
            if isinstance(value, ModuleType) and value.__name__ in module_name_registry:
                impl = module_name_registry[value.__name__]
            elif callable(value) and value.__name__ in callable_name_registry:
                impl = callable_name_registry[value.__name__]
            elif isinstance(value, Hashable) and value in eq_registry:
                impl = eq_registry[value]
            elif isinstance(value, list) and all(isinstance(v, Hashable) for v in value) and tuple(value) in eq_registry:
                impl = eq_registry[tuple(value)]
            elif value.__class__.__name__ in class_name_registry:
                impl = class_name_registry[value.__class__.__name__]
            elif hasattr(value, "__name__") and value.__name__ in type_name_registry:
                impl = type_name_registry[value.__name__]
            else:
                impl = sd.dispatch(value.__class__)
        try:
            dispatch_cache[value] = impl
        except TypeError:
            # Various value types can't be cached using weak refs
            pass
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
        # TODO: Add checks for hashable values
        if isinstance(cls, list):
            cls = tuple(cls)
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

    funcname = getattr(func, '__name__', 'larrydispatch function')
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
    # TODO: Clear both caches
    wrapper._clear_cache = dispatch_cache.clear
    update_wrapper(wrapper, func)
    return wrapper


def _dispatchcurry(dispatch_index=0, throw_if_unmatched=TypeError('Unhandled dispatch'), pre_curry=None):
    """
    An experimental dispatch approach (not currently used by retained for the time being) that can be used to
    curry values into a function call by registering functions that will provide curried values to insert the function
    call based on a value that is passed as an argument. This was ultimately abandoned in favor of using a helper
    function, but it may have utility in future features.
    """
    def decorate(func):
        spec = inspect.getfullargspec(func)

        @larrydispatch
        def curry(*args, **kwargs):
            if throw_if_unmatched:
                raise throw_if_unmatched
            return {}

        def register_module_name(name: str, fnc=None):
            if fnc is None:
                return lambda f: register_module_name(name, f)
            curry.register_module_name(name, fnc)
            return func

        def register_type_name(name, fnc=None):
            if fnc is None:
                return lambda f: register_type_name(name, f)
            curry.register_type_name(name, fnc)
            return fnc

        def register_callable_name(name, fnc=None):
            if fnc is None:
                return lambda f: register_callable_name(name, f)
            curry.register_callable_name(name, fnc)
            return fnc

        def register_class_name(name, fnc=None):
            if fnc is None:
                return lambda f: register_class_name(name, f)
            curry.register_class_name(name, fnc)
            return fnc

        def register_eq(name, fnc=None):
            if fnc is None:
                return lambda f: register_eq(name, f)
            curry.register_eq(name, fnc)
            return fnc

        def register(name, fnc=None):
            if fnc is None:
                return lambda f: register(name, f)
            curry.register(name, fnc)
            return fnc

        def normalize_args(full_spec, *args, **kwargs):
            args = list(args)
            kw = kwargs.copy()
            args = args[:len(full_spec.args)]
            positional_args = full_spec.args[:len(args)]
            for i, arg in enumerate(positional_args):
                if arg in kw:
                    args[i] = kw.pop(arg)
            return args, kw

        def wrapper(*args, **kw):
            if not args or len(args) <= dispatch_index:
                raise TypeError(f'{funcname} requires at least {dispatch_index+1} positional argument')
            if pre_curry:
                curried_values = pre_curry(*args, **kw)
                kw.update(curried_values)
                args, kw = normalize_args(spec, *args, **kw)
            cf = curry.dispatch(args[dispatch_index])
            cf_spec = inspect.getfullargspec(cf)
            cf_args, cf_kw = normalize_args(cf_spec, *args, **kw)
            curried_values = cf(*cf_args, **cf_kw)
            kw.update(curried_values)
            args, kw = normalize_args(spec, *args, **kw)
            return func(*args, **kw)

        funcname = getattr(func, '__name__', 'currydispatch function')
        wrapper.register_module_name = register_module_name
        wrapper.register_type_name = register_type_name
        wrapper.register_callable_name = register_callable_name
        wrapper.register_class_name = register_class_name
        wrapper.register_eq = register_eq
        wrapper.register = register
        wrapper.module_name_registry = curry.module_name_registry
        wrapper.callable_name_registry = curry.callable_name_registry
        wrapper.type_name_registry = curry.type_name_registry
        wrapper.class_name_registry = curry.class_name_registry
        wrapper.eq_registry = curry.eq_registry
        wrapper.registry = curry.registry
        #wrapper._clear_cache = curry._clear_cache
        update_wrapper(wrapper, func)
        return wrapper

    return decorate
