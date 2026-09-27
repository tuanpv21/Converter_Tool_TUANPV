"""
Doc Engine Backend (Redirected to app_web)
All doc generation functionality is natively embedded in app_web.py
"""
import sys

if "app_web" in sys.modules:
    current_module = sys.modules[__name__]
    app_module = sys.modules["app_web"]
    for attr in dir(app_module):
        if not attr.startswith("__"):
            setattr(current_module, attr, getattr(app_module, attr))
