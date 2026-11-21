import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
QUICKSTART = ROOT / 'examples' / 'quickstart.py'


def printed_comments(source: str) -> list[str]:
    """The comment lines that directly follow a print call: the output the example promises."""
    lines, after_print = [], False
    for line in source.splitlines():
        text = line.strip()
        if after_print and text.startswith('# '):
            lines.append(text[2:])
        else:
            after_print = 'print(' in text
    return lines


def test_quickstart_prints_what_its_comments_say():
    source = QUICKSTART.read_text()
    run = subprocess.run([sys.executable, QUICKSTART], capture_output=True, text=True, check=True)
    assert run.stdout.splitlines() == printed_comments(source)



def test_readme_shows_the_quickstart_unchanged():
    assert f'```python\n{QUICKSTART.read_text()}```' in (ROOT / 'README.md').read_text()
