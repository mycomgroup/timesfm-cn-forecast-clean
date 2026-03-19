"""中文 TimesFM 预测技能包。"""

import signal

def _ignore_background_tty_signals():
    """
    忽略 Mac/Unix 下的 SIGTTOU 和 SIGTTIN 信号。
    这能防止带有 readline/交互组件 的底层库在后台通过 `&` 运行时
    因试图触碰终端而被操作系统强行卡死 (挂起)。
    """
    try:
        # 当后台进程尝试写/配置 tty 时忽略该挂起信号
        signal.signal(signal.SIGTTOU, signal.SIG_IGN)
        # 当后台进程尝试读取 tty 时忽略该挂起信号
        signal.signal(signal.SIGTTIN, signal.SIG_IGN)
    except Exception:
        pass

# 每次导入项目时自动执行保护
_ignore_background_tty_signals()
