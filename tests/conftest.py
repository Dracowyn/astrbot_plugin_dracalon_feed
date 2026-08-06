"""让测试以「包」形式 import 插件模块，并保证 astrbot 包可导入。

- 把 data/plugins 加入 sys.path → `from astrbot_plugin_dracalon_feed import state`
- 把 AstrBot 仓库根加入 sys.path → main.py 里的 `import astrbot.api` 可用
"""

import os
import sys

PLUGIN_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PLUGINS_DIR = os.path.dirname(PLUGIN_ROOT)
ASTRBOT_ROOT = os.path.dirname(os.path.dirname(PLUGINS_DIR))
for p in (PLUGINS_DIR, ASTRBOT_ROOT):
    if p not in sys.path:
        sys.path.insert(0, p)
