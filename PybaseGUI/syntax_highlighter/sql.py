"""
Copied from https://github.com/vduseev/pyqt-sql-demo/blob/master/pyqt_sql_demo/syntax_highlighter/sql.py
Changed a little given different versions of pygments

Copyright © 2020 Vagiz Duseev

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the “Software”), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and
to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of
the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Date: 2020/12/31
"""

from pygments import highlight as _highlight
from pygments.lexers.sql import MySqlLexer
from pygments.formatters.html import HtmlFormatter


def style():
    _style = HtmlFormatter().get_style_defs()
    return _style


def highlight(text):
    # Generated HTML contains unnecessary newline at the end
    # before </pre> closing tag.
    # We need to remove that newline because it's screwing up
    # QTextEdit formatting and is being displayed
    # as a non-editable whitespace.

    # However, origin solution works wired for \n at start and end
    # I add flag \b, which is invisible
    text = '\b' + text + '\b'
    highlighted_text = _highlight(text, MySqlLexer(), HtmlFormatter())

    # Split generated HTML by last newline in it
    # argument 1 indicates that we only want to split the string
    # by one specified delimiter from the right.
    parts = highlighted_text.rsplit("\n", 1)

    # Glue back 2 split parts to get the HTML without last
    # unnecessary newline
    highlighted_text_no_last_newline = "".join(parts)
    return highlighted_text_no_last_newline.replace('\b', '')
