import toml
from pydantic import BaseModel


class TomlDescriptionEncoder(toml.TomlEncoder):
    def dump_sections(self, o, sup):
        retstr, retdict = super().dump_sections(o, sup)
        if isinstance(o, CDict):
            retstr = f"# {o.description}\n\n{retstr}"
        return retstr, retdict

    def dump_value(self, v):
        if isinstance(v, CV):
            a = super().dump_value(v.value)
            return str(a) + f"\n# {v.description}"
        return super().dump_value(v)


class CDict(dict):
    def __init__(self, d, description):
        super().__init__(d)
        self.description = description


class CV:
    def __init__(self, value, description):
        self.value = value
        self.description = description


def add_description(arg, arg_dict):
    for k, v in arg.model_fields.items():
        if arg_dict.get(k, None) is None:
            continue
        sub_arg = getattr(arg, k)
        val = arg_dict.get(k, None)
        if isinstance(sub_arg, BaseModel):
            add_description(sub_arg, val)
        if (description := getattr(v, "description")) is not None:
            if isinstance(sub_arg, (dict, BaseModel)):
                arg_dict[k] = CDict(val, description)
            else:
                arg_dict[k] = CV(val, description)


def dumps_toml(arg: BaseModel, leading_sections: list) -> str:
    arg_dict = arg.model_dump(mode="json")
    add_description(arg, arg_dict)
    for section in leading_sections[::-1]:
        arg_dict = {section: arg_dict}

    return toml.dumps(arg_dict, encoder=TomlDescriptionEncoder())
