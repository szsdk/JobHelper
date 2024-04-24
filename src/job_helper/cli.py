import pydoc

from .config import jhcfg


def console_main():
    import fire

    fire.Fire(
        {cmd: pydoc.locate(arg_class) for cmd, arg_class in jhcfg.commands.items()}
    )
