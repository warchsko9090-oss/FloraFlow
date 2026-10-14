"""One-off QA helpers for knyajestvo.ru. Do not commit."""
import re
import urllib.request

UA = {"User-Agent": "FloraFlowSiteQA/1.0"}


def fetch(url, method="GET"):
    req = urllib.request.Request(url, headers=UA, method=method)
    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            body = resp.read()
            return resp.status, dict(resp.headers), body
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers or {}), e.read() if e.fp else b""


def main():
    sm_status, _, sm = fetch("https://knyajestvo.ru/sitemap.xml")
    ids = sorted({int(m) for m in re.findall(r"/shop/plant/(\d+)", sm.decode("utf-8", "ignore"))})
    print("sitemap", sm_status, "plants", len(ids))
    ground_ok, container_ok, both_fail = [], [], []
    for pid in ids:
        s1, _, _ = fetch(f"https://knyajestvo.ru/shop/plant/{pid}")
        s2, _, _ = fetch(f"https://knyajestvo.ru/shop/plant/{pid}?seedling=1")
        if s1 == 200:
            ground_ok.append(pid)
        if s2 == 200:
            container_ok.append(pid)
        if s1 != 200 and s2 != 200:
            both_fail.append((pid, s1, s2))
        elif s1 != 200 and s2 == 200:
            print(f"  plant {pid}: ground={s1} container={s2} (needs ?seedling=1)")
    print("ground 200:", len(ground_ok))
    print("container?seedling=1 200:", len(container_ok))
    print("both fail:", both_fail[:10], "count", len(both_fail))

    # images from shop page
    st, _, html = fetch("https://knyajestvo.ru/shop")
    html_s = html.decode("utf-8", "ignore")
    imgs = set(re.findall(r'(?:src|data-src)=\"([^\"]+)\"', html_s))
    imgs |= set(re.findall(r'url\(([^)]+)\)', html_s))
    abs_imgs = []
    for u in imgs:
        u = u.strip().strip("'\"")
        if u.startswith("/"):
            u = "https://knyajestvo.ru" + u
        if u.startswith("http") and re.search(r"\.(jpg|jpeg|png|webp|svg)(\?|$)", u, re.I):
            abs_imgs.append(u)
    print("shop images found", len(abs_imgs))
    bad = []
    ok = 0
    for u in abs_imgs[:100]:
        code, _, _ = fetch(u)
        if code == 200:
            ok += 1
        else:
            bad.append((code, u[:120]))
    print("images ok", ok, "bad", len(bad))
    for row in bad[:15]:
        print(" ", row)


if __name__ == "__main__":
    main()
