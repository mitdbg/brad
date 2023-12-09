from typing import List, Optional, Any, Dict
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.font_manager as font_manager


def set_global_plt_configs(
    use_libertine_font: bool = True, font_dirs: Optional[List[str]] = None
):
    if font_dirs is None:
        inner_font_dirs = [Path.home() / ".local/share/fonts"]
    else:
        inner_font_dirs = [Path(font_dir) for font_dir in font_dirs]

    configs: Dict[str, Any] = {
        "font.size": 14,
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


COLORS = [
    "#00188d",  # 0, 24, 141 (dark blue)
    "#59b4d9",  # 89, 180, 217 (light blue)
]


if __name__ == "__main__":
    set_global_plt_configs(use_libertine_font=True)
