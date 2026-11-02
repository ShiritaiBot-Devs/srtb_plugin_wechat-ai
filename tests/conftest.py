import importlib.util
import shutil
import sys
from pathlib import Path

import pytest_asyncio
from shiritaibot.app import Application

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "wechat_ai_plugin", ROOT / "main.py", submodule_search_locations=[str(ROOT)]
)
assert spec is not None and spec.loader is not None
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)


@pytest_asyncio.fixture
async def app(tmp_path):
    # Install into the user's plugin directory, not the host source tree.
    directory = tmp_path / "plugins" / "wechat"
    directory.mkdir(parents=True)
    for name in ["plugin.toml", "main.py", "mcp_client.py"]:
        shutil.copy2(ROOT / name, directory / name)
    application = Application(tmp_path)
    await application.start()
    try:
        yield application
    finally:
        await application.close()
