"""
Build report/final_report.pdf from final_report.html (AUCA template layout).
Two passes, like LaTeX: pass 1 renders with placeholder page numbers, we find the page each
heading lands on (pdftotext), then pass 2 renders with the real numbers in the contents page.
Requires: Google Chrome, poppler (pdftotext).
"""
import html, os, re, subprocess, unicodedata

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "final_report.html")
BUILD = os.path.join(HERE, ".build.html")
PDF = os.path.join(HERE, "final_report.pdf")
CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

src = open(SRC, encoding="utf-8").read()
heads = re.findall(r'<h([23])[^>]*data-toc="(\d)"[^>]*>(.*?)</h\1>', src, re.S)
entries = []
for _, lvl, inner in heads:
    m = re.match(r'\s*<span class="num">([^<]*)</span>(.*)', inner, re.S)
    num, title = (m.group(1), m.group(2)) if m else ("", inner)
    title = html.unescape(re.sub(r"<[^>]+>", "", title)).strip()
    entries.append((int(lvl), num, title))


def toc_html(pages):
    out = []
    for (lvl, num, title), pg in zip(entries, pages):
        out.append(f'<div class="l{lvl}"><span class="num">{num}</span><span class="t">{html.escape(title)}</span>'
                   f'<span class="dots"></span><span class="pg">{pg}</span></div>')
    return "\n".join(out)


def render(pages):
    open(BUILD, "w", encoding="utf-8").write(src.replace("<!--TOC-->", toc_html(pages)))
    subprocess.run([CHROME, "--headless=new", "--disable-gpu", "--no-pdf-header-footer",
                    "--allow-file-access-from-files", f"--print-to-pdf={PDF}", "file://" + BUILD],
                   check=True, capture_output=True)


def norm(s):
    s = unicodedata.normalize("NFKC", s)          # ligatures fi/fl -> letters
    return re.sub(r"[^a-z0-9]", "", s.lower())


render(["00"] * len(entries))
n_pages = int(re.search(r"Pages:\s+(\d+)", subprocess.run(["pdfinfo", PDF], capture_output=True, text=True).stdout).group(1))
texts = [norm(subprocess.run(["pdftotext", "-f", str(i), "-l", str(i), "-layout", PDF, "-"],
                             capture_output=True, text=True).stdout) for i in range(1, n_pages + 1)]
toc_last = max(i for i, t in enumerate(texts) if t.startswith("contents") or "contents" in t[:200])
pages, start = [], toc_last + 1
for lvl, num, title in entries:
    key = norm(num + title)[:28]
    found = next((i for i in range(start, n_pages) if key in texts[i]), None)
    if found is None:
        raise SystemExit(f"heading not found in PDF: {num} {title}")
    pages.append(str(found + 1))
    start = found
render(pages)
os.remove(BUILD)
print(f"{PDF}: {n_pages} pages")
for (lvl, num, title), pg in zip(entries, pages):
    print(f"  {'  ' * (lvl - 1)}{num:5} {title[:60]:60} {pg}")
