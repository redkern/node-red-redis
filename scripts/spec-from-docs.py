# Converts a Claude Docs prose XML read into Markdown (used once to refresh docs/spec.md).
import json, re, sys
raw = open(sys.argv[1]).read()
try:
    data = json.loads(raw)
    xml = data['data']['xml']
except Exception:
    xml = raw
def inline(s):
    s = re.sub(r"<code>(.*?)</code>", lambda m: '`' + m.group(1) + '`', s, flags=re.S)
    s = re.sub(r"<bold>(.*?)</bold>", lambda m: '**' + m.group(1) + '**', s, flags=re.S)
    s = re.sub(r"<date[^>]*value='([^']*)'[^>]*/>", r"\1", s)
    s = re.sub(r"<mention[^>]*name='([^']*)'[^>]*/>", r"@\1", s)
    s = re.sub(r'<!\[CDATA\[(.*?)\]\]>', lambda m: m.group(1).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;'), s, flags=re.S)
    s = re.sub(r'<[^>]+>', '', s)
    for a, b in [('&gt;', '>'), ('&lt;', '<'), ('&quot;', '"'), ('&apos;', "'"), ('&amp;', '&')]:
        s = s.replace(a, b)
    return s
out = []
pos = 0
token = re.compile(r"<(paragraph|table|list|codeBlock)\b([^>]*)>(.*?)</\1>", re.S)
def render_list(attrs, body, depth=0):
    lines = []
    items = re.findall(r"<listItem[^>]*>(.*?)</listItem>", body, re.S)
    for item in items:
        paras = re.findall(r"<paragraph[^>]*>(.*?)</paragraph>", item, re.S)
        lines.append('  ' * depth + '- ' + inline(paras[0] if paras else ''))
    return '\n'.join(lines)
for m in token.finditer(xml):
    kind, attrs, body = m.group(1), m.group(2), m.group(3)
    if kind == 'paragraph':
        h = re.search(r"heading='(\d)'", attrs)
        text = inline(body)
        out.append(('#' * int(h.group(1)) + ' ' if h else '') + text)
    elif kind == 'list':
        out.append(render_list(attrs, body))
    elif kind == 'codeBlock':
        out.append('```\n' + inline(body) + '\n```')
    elif kind == 'table':
        rows = re.findall(r"<row[^>]*>(.*?)</row>", body, re.S)
        lines = []
        for index, row in enumerate(rows):
            cells = [inline(c).strip().replace('|', '\\|') for c in re.findall(r"<cell[^>]*>(.*?)</cell>", row, re.S)]
            lines.append('| ' + ' | '.join(cells) + ' |')
            if index == 0:
                lines.append('| ' + ' | '.join(['---'] * len(cells)) + ' |')
        out.append('\n'.join(lines))
print('\n\n'.join(out))
