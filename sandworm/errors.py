class SandwormError(Exception):
    pass


class NoEnvironmentError(SandwormError):
    pass


class SecondMainTargetError(SandwormError):
    pass
