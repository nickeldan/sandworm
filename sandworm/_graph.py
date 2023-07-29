import collections
import enum

from . import target


class VisitState(enum.Enum):
    NOT_VISITED = enum.auto()
    VISITED = enum.auto()
    IN_STACK = enum.auto()


def dfs(
    stack: list[target.Target], visited: collections.abc.Mapping[target.Target, VisitState]
) -> list[target.Target] | None:
    top = stack[-1]
    visited[top] = VisitState.IN_STACK

    for dep in top.dependencies:
        if (state := visited[dep]) == VisitState.IN_STACK:
            return stack[stack.index(dep) :]
        elif state == VisitState.NOT_VISITED:
            stack.append(dep)
            if (cycle := dfs(stack, visited)) is not None:
                return cycle
            stack.pop(-1)

    visited[top] = VisitState.VISITED
    return None


def detect_cycle(root_node: target.Target) -> list[target.Target] | None:
    visited = collections.defaultdict(lambda: VisitState.NOT_VISITED)
    visited[root_node] = VisitState.IN_STACK

    return dfs([root_node], visited)
