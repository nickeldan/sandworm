class SandwormError(Exception):
    pass


class NoEnvironmentError(SandwormError):
    pass


class RepeatedTargetError(SandwormError):
    pass


class SecondMainTargetError(SandwormError):
    pass
