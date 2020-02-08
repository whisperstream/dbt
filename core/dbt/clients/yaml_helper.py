import dbt.exceptions

import os

import yaml
import yaml.scanner


YAML_ERROR_MESSAGE = """
Syntax error near line {line_number}
------------------------------
{nice_error}

Raw Error:
------------------------------
{raw_error}
""".strip()


class Loader(yaml.SafeLoader):
    def __init__(self, stream):
        self._root = os.path.split(stream.name)[0]
        super(Loader, self).__init__(stream)

    def include(self, node):
        filename = os.path.join(self._root, self.construct_scalar(node))
        with open(filename, 'r') as f:
            return yaml.load(f, Loader)


Loader.add_constructor('!include', Loader.include)


def line_no(i, line, width=3):
    line_number = str(i).ljust(width)
    return "{}| {}".format(line_number, line)


def prefix_with_line_numbers(string, no_start, no_end):
    line_list = string.split('\n')

    numbers = range(no_start, no_end)
    relevant_lines = line_list[no_start:no_end]

    return "\n".join([
        line_no(i + 1, line) for (i, line) in zip(numbers, relevant_lines)
    ])


def contextualized_yaml_error(raw_contents, error):
    mark = error.problem_mark

    min_line = max(mark.line - 3, 0)
    max_line = mark.line + 4

    nice_error = prefix_with_line_numbers(raw_contents, min_line, max_line)

    return YAML_ERROR_MESSAGE.format(line_number=mark.line + 1,
                                     nice_error=nice_error,
                                     raw_error=error)


def load_yaml_text(contents):
    try:
        if has_attr(contents, 'name') and has_attr(contents, 'read'):
            return yaml.load(contents, Loader)
        else:
            return yaml.safe_load(contents)
    except (yaml.scanner.ScannerError, yaml.YAMLError) as e:
        if hasattr(e, 'problem_mark'):
            error = contextualized_yaml_error(contents, e)
        else:
            error = str(e)

        raise dbt.exceptions.ValidationException(error)
