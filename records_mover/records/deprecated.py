import warnings


def warn_deprecated(**kwargs) -> None:
    for name, value in kwargs.items():
        if value is not None:
            warnings.simplefilter('always', DeprecationWarning)  # turn off filter
            warnings.warn(f"{name} argument is deprecated, will not be used, and "
                          "will be removed in a future release",
                          category=DeprecationWarning,
                          stacklevel=4)
            warnings.simplefilter('default', DeprecationWarning)  # reset filter
