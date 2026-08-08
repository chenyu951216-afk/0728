from pathlib import Path

import uvicorn
from fastapi.responses import HTMLResponse

from app import PORT, app

# Replace the minimal diagnostic root page with the responsive dashboard while
# leaving every learning/trading API and background worker in app.py untouched.
app.router.routes = [route for route in app.router.routes if getattr(route, "path", None) != "/"]


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    return Path("dashboard.html").read_text(encoding="utf-8")


if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=PORT)
