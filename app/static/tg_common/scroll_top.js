/* Кнопка «Наверх» для всех Telegram Mini App. */
(function () {
  if (window.__ffScrollTopReady) return;
  window.__ffScrollTopReady = true;

  var THRESHOLD = 240;

  function injectCss() {
    if (document.getElementById("ff-scroll-top-css")) return;
    var style = document.createElement("style");
    style.id = "ff-scroll-top-css";
    style.textContent = [
      "#ffScrollTop{",
      "position:fixed;z-index:45;",
      "left:max(14px,env(safe-area-inset-left));",
      "bottom:max(20px,env(safe-area-inset-bottom));",
      "min-width:48px;height:48px;padding:0 14px;",
      "border:0;border-radius:999px;",
      "background:rgba(17,24,20,.88);color:#fffdf8;",
      "font:650 13px/1 -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;",
      "box-shadow:0 8px 20px rgba(17,24,20,.22);",
      "opacity:0;pointer-events:none;transform:translateY(8px);",
      "transition:opacity .18s ease,transform .18s ease;",
      "-webkit-tap-highlight-color:transparent;touch-action:manipulation;",
      "}",
      "#ffScrollTop.is-on{opacity:1;pointer-events:auto;transform:none;}",
      "#ffScrollTop:active{filter:brightness(1.12);}",
    ].join("");
    document.head.appendChild(style);
  }

  function scrollY() {
    return (
      window.pageYOffset ||
      document.documentElement.scrollTop ||
      document.body.scrollTop ||
      0
    );
  }

  function scrollRoot() {
    if (document.scrollingElement) return document.scrollingElement;
    return document.documentElement || document.body;
  }

  function goTop() {
    try {
      window.scrollTo({ top: 0, behavior: "smooth" });
    } catch (_) {
      window.scrollTo(0, 0);
    }
    var root = scrollRoot();
    if (root && root !== document.body) {
      try {
        root.scrollTo({ top: 0, behavior: "smooth" });
      } catch (_) {
        root.scrollTop = 0;
      }
    }
    if (document.body) document.body.scrollTop = 0;
  }

  function sync(btn) {
    if (scrollY() >= THRESHOLD) btn.classList.add("is-on");
    else btn.classList.remove("is-on");
  }

  function boot() {
    injectCss();
    var btn = document.getElementById("ffScrollTop");
    if (!btn) {
      btn = document.createElement("button");
      btn.type = "button";
      btn.id = "ffScrollTop";
      btn.setAttribute("aria-label", "Наверх");
      btn.textContent = "↑ Наверх";
      document.body.appendChild(btn);
    }
    btn.addEventListener("click", function (e) {
      e.preventDefault();
      goTop();
    });
    var onScroll = function () { sync(btn); };
    window.addEventListener("scroll", onScroll, { passive: true });
    document.addEventListener("scroll", onScroll, { passive: true, capture: true });
    sync(btn);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
