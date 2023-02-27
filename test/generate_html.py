import os
import random
from typing import Iterable, NamedTuple

HTML_FORMAT = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>{name}</title>
</head>
<body>
{anchors}
</body>
</html>
""".strip()

ANCHOR_FORMAT = '   <a href="./{name}">{name}</a>'


def create_html(name_src: str, names: list[str]):
    anchors = '\n'.join(ANCHOR_FORMAT.format(name=name) for name in names)
    html = HTML_FORMAT.format(name=name_src, anchors=anchors)
    return html


class TestCaseGenerationFailureError(ValueError):
    pass


def generate_mapping(
        num_htmls: int,
        num_links_mean: float,
        num_links_sigma: float,
        seed: int = 42
) -> dict[str, list[str]]:
    rand = random.Random(seed)

    names = [f'{i}.html' for i in range(round(num_htmls * 2))]

    def num_links():
        return round(rand.gauss(num_links_mean, num_links_sigma))

    src_name = names[0]
    mapping = {None: [src_name]}
    reachable = set()
    while len(mapping) < num_htmls:
        dst_names = rand.choices(names, k=num_links())
        mapping[src_name] = dst_names
        reachable = (reachable | set(dst_names)) - mapping.keys()
        if not reachable:
            raise TestCaseGenerationFailureError(f'failed to generate test case for {seed=}')
        src_name = reachable.pop()

    return mapping


# TODO: this is called twice in the same context
def iter_htmls_from_mapping(mapping: dict[str, list[str]]) -> Iterable[tuple[str, str]]:
    for src_name, names in mapping.items():
        if src_name is None:
            continue
        yield src_name, create_html(src_name, names)


TEST_HTML_DIR_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'test_html')


class AnswerEntry(NamedTuple):
    name: str
    back_name: str
    content: str


def create_answer(mapping: dict[str, list[str]]) -> dict[tuple[str, str], AnswerEntry]:
    answers = {}
    htmls = dict(iter_htmls_from_mapping(mapping))
    for src_name, names in mapping.items():
        for name in names:
            answer = AnswerEntry(
                name=name,
                back_name=src_name,
                content=htmls.get(name)
            )
            answers[(src_name, name)] = answer
    return answers


def create_test_case(**kwargs):
    mapping = generate_mapping(**kwargs)
    files = dict(iter_htmls_from_mapping(mapping))
    answers = create_answer(mapping)
    return files, answers
