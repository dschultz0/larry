def copy_non_null_keys(param_list):
    result = {}
    for key, val in param_list.items():
        if val is not None:
            result[key] = val
    return result


def map_parameters(parameters, key_map):
    result = {}
    for k, i in key_map.items():
        if parameters.get(k) is not None:
            result[i] = parameters[k]
    return result
