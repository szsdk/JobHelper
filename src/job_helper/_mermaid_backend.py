from __future__ import annotations

import base64
import urllib.request
import zlib
from pathlib import Path


def flowchart(nodes: dict[str, str], links: dict[tuple[str, str], str]):
    node_styles = {
        "norun": "classDef norun fill:#ddd,stroke:#aaa,stroke-width:3px,stroke-dasharray: 5 5",
        "failed": "classDef failed fill:#eaa,stroke:#e44",
        "completed": "classDef completed fill:#aea,stroke:#4a4",
    }
    link_styles = {
        "after": "--o",
        "afterany": "-.-o",
        "afternotok": "-.-x",
        "afterok": "-->",
    }

    flow = ["flowchart TD"]
    for (job_a, job_b), link in links.items():
        a = job_a if job_a not in nodes else f"{job_a}:::{nodes[job_a]}"
        b = job_b if job_b not in nodes else f"{job_b}:::{nodes[job_b]}"
        flow.append(f"    {a} {link_styles[link]} {b}")
    flow.extend(list(node_styles.values()))
    return "\n".join(flow)


_HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
  <body>
    <pre class="mermaid">
{mermaid_code}
    </pre>
    <script type="module">
      import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
    </script>
  </body>
</html>"""


def render_chart(chart: str, output_fn: str):
    if output_fn == "-":
        print(chart)
        return
    output = Path(output_fn)
    if output.suffix == ".html":
        with output.open("w") as fp:
            print(_HTML_TEMPLATE.format(mermaid_code=chart), file=fp)
        return
    url = base64.urlsafe_b64encode(zlib.compress(chart.encode(), 9)).decode("ascii")
    if output.suffix == ".png":
        url = "https://kroki.io/mermaid/png/" + url
    elif output.suffix == ".svg":
        url = "https://kroki.io/mermaid/svg/" + url
    else:
        raise ValueError(f"Unsupported output format: {output.suffix}")
    print(url)
    hdr = {"User-Agent": "Mozilla/5.0"}
    req = urllib.request.Request(url, headers=hdr)
    with urllib.request.urlopen(req) as response, output.open("wb") as fp:
        fp.write(response.read())
