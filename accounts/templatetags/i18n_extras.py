from django import template
from urllib.parse import urlsplit

register = template.Library()


@register.simple_tag(takes_context=True)
def switch_language_url(context, lang_code):
    """Return current path with the first URL segment replaced by lang_code.

    Works with i18n_patterns-style language prefixes (e.g., /ru/... or /tk/...).
    Preserves the query string.
    """
    request = context.get("request")
    full_path = request.get_full_path() if request is not None else "/"

    split = urlsplit(full_path)
    path = split.path or "/"

    # Normalize and split path
    leading_slash = path.startswith("/")
    trailing_slash = path.endswith("/") and len(path) > 1
    parts = [p for p in path.strip("/").split("/") if p]

    if parts and parts[0] in {"ru", "tk"}:
        parts[0] = lang_code
    else:
        parts = [lang_code] + parts

    new_path = "/" + "/".join(parts)
    if trailing_slash and not new_path.endswith("/"):
        new_path += "/"

    if split.query:
        new_path = f"{new_path}?{split.query}"

    return new_path


