import collections
import enum

from . import target


class VisitState(enum.Enum):
    NOT_VISITED = enum.auto()
    VISITED = enum.auto()
    IN_STACK = enum.auto()


class Graph:
    def __init__(self, root_node: target.Target) -> None:
        self.visited: collections.abc.MutableMapping[target.Target, VisitState] = collections.defaultdict(
            lambda: VisitState.NOT_VISITED
        )
        self.visited[root_node] = VisitState.IN_STACK
        self.stack: list[target.Target] = [root_node]

    def find_cycle(self) -> list[target.Target] | None:
        top = self.stack[-1]
        self.visited[top] = VisitState.IN_STACK

        for dep in top.dependencies:
            match self.visited[dep]:
                case VisitState.IN_STACK:
                    return self.stack[self.stack.index(dep) :]
                case VisitState.NOT_VISITED:
                    self.stack.append(dep)
                    if (cycle := self.find_cycle()) is not None:
                        return cycle
                    self.stack.pop(-1)

        self.visited[top] = VisitState.VISITED
        return None
