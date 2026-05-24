"""
kimi_review.py
~~~~~~~~~~~~~~
使用 Moonshot Kimi 视觉模型对候选股票进行图表分析评分。

用法：
    python agent/kimi_review.py
    python agent/kimi_review.py --config config/kimi_review.yaml

环境变量：
    MOONSHOT_API_KEY  —— Moonshot/Kimi API Key（必填）
"""

from __future__ import annotations

import argparse
import base64
import os
import sys
from pathlib import Path
from typing import Any

from openai import OpenAI
import yaml

from base_reviewer import BaseReviewer

_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_CONFIG_PATH = _ROOT / "config" / "kimi_review.yaml"

DEFAULT_CONFIG: dict[str, Any] = {
    "candidates": "data/candidates/candidates_latest.json",
    "kline_dir": "data/kline",
    "output_dir": "data/review",
    "prompt_path": "agent/prompt.md",
    "model": "kimi-k2.6",
    "base_url": "https://api.moonshot.ai/v1",
    "request_delay": 5,
    "skip_existing": False,
    "suggest_min_score": 4.0,
}


def _resolve_cfg_path(path_like: str | Path, base_dir: Path = _ROOT) -> Path:
    p = Path(path_like)
    return p if p.is_absolute() else (base_dir / p)


def load_config(config_path: Path | None = None) -> dict[str, Any]:
    cfg_path = config_path or _DEFAULT_CONFIG_PATH
    if not cfg_path.exists():
        raise FileNotFoundError(f"找不到配置文件：{cfg_path}")

    with open(cfg_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    cfg = {**DEFAULT_CONFIG, **raw}
    cfg["candidates"] = _resolve_cfg_path(cfg["candidates"])
    cfg["kline_dir"] = _resolve_cfg_path(cfg["kline_dir"])
    cfg["output_dir"] = _resolve_cfg_path(cfg["output_dir"])
    cfg["prompt_path"] = _resolve_cfg_path(cfg["prompt_path"])
    return cfg


class KimiReviewer(BaseReviewer):
    def __init__(self, config):
        super().__init__(config)

        api_key = os.environ.get("MOONSHOT_API_KEY", "")
        if not api_key:
            print("[ERROR] 未找到环境变量 MOONSHOT_API_KEY，请先设置后重试。", file=sys.stderr)
            sys.exit(1)

        self.client = OpenAI(
            api_key=api_key,
            base_url=self.config.get("base_url", "https://api.moonshot.ai/v1"),
        )

    @staticmethod
    def image_to_data_url(path: Path) -> str:
        suffix = path.suffix.lower()
        mime_map = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png"}
        mime_type = mime_map.get(suffix, "image/jpeg")
        encoded = base64.b64encode(path.read_bytes()).decode("ascii")
        return f"data:{mime_type};base64,{encoded}"

    def review_stock(self, code: str, day_chart: Path, prompt: str) -> dict:
        user_text = (
            f"股票代码：{code}\n\n"
            "以下是该股票的日线图，请按照系统提示中的框架进行分析，"
            "并严格输出一个 JSON 对象，不要包含 Markdown 代码块。"
        )

        response = self.client.chat.completions.create(
            model=self.config.get("model", "kimi-k2.6"),
            temperature=0.2,
            messages=[
                {"role": "system", "content": prompt},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": user_text},
                        {
                            "type": "image_url",
                            "image_url": {"url": self.image_to_data_url(day_chart)},
                        },
                    ],
                },
            ],
        )

        response_text = response.choices[0].message.content
        if not response_text:
            raise RuntimeError(f"Kimi 返回空响应，无法解析 JSON（code={code}）")

        result = self.extract_json(response_text)
        result["code"] = code
        return result


def main():
    parser = argparse.ArgumentParser(description="Kimi 图表复评")
    parser.add_argument(
        "--config",
        default=str(_DEFAULT_CONFIG_PATH),
        help="配置文件路径（默认 config/kimi_review.yaml）",
    )
    args = parser.parse_args()

    config = load_config(Path(args.config))
    reviewer = KimiReviewer(config)
    reviewer.run()


if __name__ == "__main__":
    main()
