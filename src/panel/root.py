"""
根路由模块 - 处理控制面板主页
"""
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse

from log import log
from .utils import is_mobile_user_agent


# 创建路由器
router = APIRouter(tags=["root"])


def _inject_frontend_patches(html_content: str) -> str:
    """注入前端补丁脚本与缓存版本，避免 HTML 与 JS 缓存版本不一致。"""
    try:
        version = "usage-hourly-stacked-v1"
        html_content = html_content.replace(
            '<script src="./front/common.js"></script>',
            f'<script src="./front/common.js?v={version}"></script>'
        )
        patch_script = f'<script src="./front/usage_stats_enhanced.js?v={version}"></script>'
        if "usage_stats_enhanced.js" not in html_content and "</body>" in html_content:
            html_content = html_content.replace("</body>", f"    {patch_script}\n</body>")
        return html_content
    except Exception as e:
        log.warning(f"注入前端补丁失败，返回原始控制面板: {e}")
        return html_content


@router.get("/", response_class=HTMLResponse)
async def serve_control_panel(request: Request):
    """提供统一控制面板"""
    try:
        user_agent = request.headers.get("user-agent", "")
        is_mobile = is_mobile_user_agent(user_agent)

        if is_mobile:
            html_file_path = "front/control_panel_mobile.html"
        else:
            html_file_path = "front/control_panel.html"

        with open(html_file_path, "r", encoding="utf-8") as f:
            html_content = f.read()
        return HTMLResponse(content=_inject_frontend_patches(html_content))

    except Exception as e:
        log.error(f"加载控制面板页面失败: {e}")
        raise HTTPException(status_code=500, detail="服务器内部错误")
