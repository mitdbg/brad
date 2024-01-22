import pathlib
from typing import List, Optional, Any, Dict

import matplotlib.pyplot as plt
import matplotlib.font_manager as font_manager


def set_global_plt_configs(
    use_libertine_font: bool = True, font_dirs: Optional[List[str]] = None
):
    if font_dirs is None:
        inner_font_dirs = [pathlib.Path.home() / ".local/share/fonts"]
    else:
        inner_font_dirs = [pathlib.Path(font_dir) for font_dir in font_dirs]

    configs: Dict[str, Any] = {
        "font.size": 16,
        "pdf.fonttype": 42,  # Ensures type 1 fonts, apparently.
    }

    # Matches the font used in the VLDB template (circa 2023).
    if use_libertine_font:
        font_files = font_manager.findSystemFonts(fontpaths=inner_font_dirs)
        font_manager_instance = font_manager.FontManager()
        for font in font_files:
            font_manager_instance.addfont(font)
        configs["font.family"] = "Linux Libertine"

    plt.rcParams.update(configs)


COLORS = {
    "brad_dark_blue": "#00188d",  # 0, 24, 141 (dark blue)
    "brad_light_blue": "#59b4d9",  # 89, 180, 217 (light blue)
    "redshift_aurora": "#c03221",  # (orange-red)
    "tidb": "#3f826d",  # (teal)
    "hand": "#f7b267",  # (orange)
}
