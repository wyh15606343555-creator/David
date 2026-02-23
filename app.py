"""
财务报表智能生成平台 — Demo v0.3
================================
面向中色华鑫马本德矿业有限公司
央企级 · 智能财务 · 多币种 · AI驱动
"""

import streamlit as st
import pandas as pd
import os
import sqlite3
from datetime import datetime
from pathlib import Path
import io
import time

# ── 页面配置 ──
st.set_page_config(
    page_title="财务报表智能生成平台",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# ====================================================================
# 工具函数
# ====================================================================
def get_api_key(key_name: str):
    try:
        return st.secrets[key_name]
    except Exception:
        pass
    val = os.environ.get(key_name)
    if val:
        return val
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                if k.strip() == key_name:
                    return v.strip()
    return None


def format_cell(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    if isinstance(x, (int, float)):
        if float(x) == int(x):
            return f"{int(x)}"
        return f"{x:.2f}"
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return ""
        try:
            num = float(s)
            if num == int(num):
                return f"{int(num)}"
            return f"{num:.2f}"
        except (ValueError, OverflowError):
            pass
    return str(x)


def prepare_for_display(df):
    display_df = df.copy()
    new_cols = []
    for i, c in enumerate(display_df.columns):
        s = str(c)
        new_cols.append(f"列{i+1}" if s.startswith("Unnamed") else s)
    display_df.columns = new_cols
    for col in display_df.columns:
        if display_df[col].dtype == object:
            display_df[col] = display_df[col].ffill()
    for col in display_df.columns:
        display_df[col] = display_df[col].apply(format_cell)
    return display_df


def period_options():
    now = datetime.now()
    opts = []
    for i in range(24):
        y, m = now.year, now.month - i
        while m <= 0:
            m += 12
            y -= 1
        opts.append(f"{y:04d}-{m:02d}")
    return opts


def period_label(p: str) -> str:
    if not p or p == "全部月份":
        return "全部月份"
    try:
        y, m = p.split("-")
        return f"{y}年{m}月"
    except Exception:
        return p


def period_data_dir(period):
    d = DATA_DIR / period.replace("-", "")
    d.mkdir(exist_ok=True)
    return d


def period_output_dir(period):
    d = OUTPUT_DIR / period.replace("-", "")
    d.mkdir(exist_ok=True)
    return d


def fmt_size(b):
    if b < 1024:
        return f"{b} B"
    elif b < 1024 ** 2:
        return f"{b / 1024:.1f} KB"
    else:
        return f"{b / 1024 ** 2:.1f} MB"


def read_excel_file(path_or_bytes, ext: str) -> dict:
    if ext == "csv":
        if isinstance(path_or_bytes, (str, Path)):
            return {"Sheet1": pd.read_csv(path_or_bytes)}
        return {"Sheet1": pd.read_csv(io.BytesIO(path_or_bytes))}
    engine = "xlrd" if ext == "xls" else "openpyxl"
    if isinstance(path_or_bytes, (str, Path)):
        xls = pd.ExcelFile(path_or_bytes, engine=engine)
    else:
        xls = pd.ExcelFile(io.BytesIO(path_or_bytes), engine=engine)
    return {name: xls.parse(name) for name in xls.sheet_names}


# ====================================================================
# 路径 & 数据库
# ====================================================================
BASE_DIR     = Path(__file__).parent
DATA_DIR     = BASE_DIR / "data"
OUTPUT_DIR   = BASE_DIR / "output"
MAPPING_DIR  = BASE_DIR / "mappings"
TEMPLATE_DIR = BASE_DIR / "templates"
DB_PATH      = DATA_DIR / "platform.db"

for _d in [DATA_DIR, OUTPUT_DIR, MAPPING_DIR, TEMPLATE_DIR]:
    _d.mkdir(exist_ok=True)


def init_db():
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS uploads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        period TEXT NOT NULL DEFAULT '',
        filename TEXT NOT NULL,
        file_type TEXT,
        sheet_count INTEGER DEFAULT 0,
        row_count INTEGER DEFAULT 0,
        upload_time TEXT NOT NULL,
        file_path TEXT,
        status TEXT DEFAULT '已上传'
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS generations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        period TEXT NOT NULL DEFAULT '',
        source_upload_id INTEGER,
        ai_model TEXT,
        output_filename TEXT,
        output_path TEXT,
        status TEXT DEFAULT '生成中',
        created_at TEXT NOT NULL,
        duration_seconds REAL
    )""")
    for tbl in ("uploads", "generations"):
        cols = [r[1] for r in c.execute(f"PRAGMA table_info({tbl})").fetchall()]
        if "period" not in cols:
            c.execute(f"ALTER TABLE {tbl} ADD COLUMN period TEXT NOT NULL DEFAULT ''")
    conn.commit()
    conn.close()


init_db()


def get_db():
    return sqlite3.connect(str(DB_PATH))


# ====================================================================
# 报表模块结构
# ====================================================================
REPORT_MODULES = {
    "⚡ 月度快报": [
        "生产经营月度快报",
        "资金情况月报",
        "重要指标快报",
        "产销存快报",
        "成本费用快报",
        "人力资源快报",
        "环保安全快报",
    ],
    "📋 基础报表": [
        "资产负债表",
        "利润表",
        "现金流量表",
        "所有者权益变动表",
        "应收账款明细表",
        "应付账款明细表",
        "存货明细表",
        "固定资产明细表",
        "长期投资明细表",
        "铜产销成本表",
        "管理费用明细表",
        "财务费用明细表",
    ],
    "🔍 分析底稿": [
        "同比分析底稿",
        "环比分析底稿",
        "预算执行分析",
        "成本构成分析",
        "费用明细分析",
        "资产负债分析",
        "毛利率趋势分析",
        "现金流量分析",
    ],
    "🗄️ 基础资料库": [],   # 动态：来自已上传文件
    "📝 Word报告": [
        "月度财务分析报告（完整版）",
    ],
}


def gen_demo_df(report_name: str, currency: str) -> pd.DataFrame:
    """根据报表名称生成演示数据"""
    unit = "万美元" if currency == "美元" else "万元"

    if "资产负债" in report_name:
        return pd.DataFrame({
            "项  目": [
                "一、流动资产", "  货币资金", "  应收账款", "  预付账款", "  存货", "  其他流动资产",
                "  流动资产合计", "",
                "二、非流动资产", "  固定资产", "  累计折旧", "  固定资产净值", "  无形资产",
                "  非流动资产合计", "",
                "资  产  总  计", "",
                "一、流动负债", "  短期借款", "  应付账款", "  预收账款", "  其他流动负债",
                "  流动负债合计", "",
                "二、非流动负债", "  长期借款",
                "  非流动负债合计", "",
                "负  债  合  计", "",
                "实收资本", "未分配利润", "所有者权益合计", "",
                "负债和所有者权益合计",
            ],
            f"期末余额（{unit}）": [
                "", "12,450", "8,320", "1,800", "15,680", "510",
                "38,760", "",
                "", "95,430", "(3,420)", "92,010", "2,140",
                "94,150", "",
                "132,910", "",
                "", "5,200", "3,840", "680", "2,930",
                "12,650", "",
                "", "45,000",
                "45,000", "",
                "57,650", "",
                "30,000", "45,260", "75,260", "",
                "132,910",
            ],
            f"期初余额（{unit}）": [
                "", "10,230", "7,890", "1,450", "14,320", "1,290",
                "35,180", "",
                "", "92,100", "(2,640)", "89,460", "2,380",
                "91,840", "",
                "127,020", "",
                "", "4,800", "3,560", "540", "2,300",
                "11,200", "",
                "", "42,000",
                "42,000", "",
                "53,200", "",
                "30,000", "43,820", "73,820", "",
                "127,020",
            ],
            "增减幅度": [
                "", "↑21.7%", "↑5.5%", "↑24.1%", "↑9.5%", "↓60.5%",
                "↑10.2%", "",
                "", "↑3.6%", "", "↑2.9%", "↓10.1%",
                "↑2.5%", "",
                "↑4.6%", "",
                "", "↑8.3%", "↑7.9%", "↑25.9%", "↑27.4%",
                "↑12.9%", "",
                "", "↑7.1%",
                "↑7.1%", "",
                "↑8.4%", "",
                "—", "↑3.3%", "↑2.0%", "",
                "↑4.6%",
            ],
        })

    elif "利润" in report_name:
        return pd.DataFrame({
            "项  目": [
                "一、营业收入",
                "减：营业成本", "    营业税金及附加", "    销售费用",
                "    管理费用", "    财务费用", "    资产减值损失",
                "加：公允价值变动收益", "    投资收益",
                "二、营业利润",
                "加：营业外收入", "减：营业外支出",
                "三、利润总额", "减：所得税费用",
                "四、净  利  润",
            ],
            f"本期金额（{unit}）": [
                "85,420",
                "62,180", "850", "1,240",
                "3,680", "1,120", "—",
                "—", "230",
                "16,580",
                "420", "180",
                "16,820", "2,523",
                "14,297",
            ],
            f"上年同期（{unit}）": [
                "78,930",
                "58,640", "790", "1,180",
                "3,420", "980", "—",
                "—", "180",
                "14,100",
                "360", "210",
                "14,250", "2,138",
                "12,112",
            ],
            "同比增减": [
                "↑8.2%",
                "↑6.0%", "↑7.6%", "↑5.1%",
                "↑7.6%", "↑14.3%", "—",
                "—", "↑27.8%",
                "↑17.6%",
                "↑16.7%", "↓14.3%",
                "↑18.0%", "↑18.0%",
                "↑18.0%",
            ],
        })

    elif "快报" in report_name or "指标" in report_name:
        return pd.DataFrame({
            "指  标": [
                "铜产量（吨）", "铜销量（吨）", "综合回收率（%）",
                "电耗（度/吨铜）", "生产成本（美元/吨）",
                "销售收入（万美元）", "净利润（万美元）",
                "员工人数（人）",
            ],
            "本月完成": ["2,086", "2,140", "91.3%", "1,850", "4,280", "20,330", "1,430", "486"],
            "月度计划": ["2,100", "2,100", "91.0%", "1,900", "4,350", "20,000", "1,400", "490"],
            "完  成  率": ["99.3%", "101.9%", "✅达标", "✅达标", "✅达标", "✅达标", "✅达标", "99.2%"],
            "上月实际": ["2,050", "2,095", "90.8%", "1,870", "4,310", "19,820", "1,380", "485"],
            "环  比": ["↑1.8%", "↑2.1%", "↑0.5%", "↓1.1%", "↓0.7%", "↑2.6%", "↑3.6%", "↑0.2%"],
            "本年累计": ["12,380", "12,590", "—", "—", "—", "119,450", "8,640", "—"],
        })

    elif "产销存" in report_name:
        return pd.DataFrame({
            "项  目": [
                "一、生产", "  铜产量（吨）", "  硫酸联产量（吨）", "  综合回收率（%）", "",
                "二、销售", "  铜销量（吨）", "  铜销售收入（万美元）", "  铜均价（美元/吨）", "",
                "三、库存", "  铜库存（吨）", "  原料库存（吨）", "  硫酸库存（吨）",
            ],
            "本  月": ["", "2,086", "8,240", "91.3%", "", "", "2,140", "20,330", "9,500", "", "", "450", "12,300", "3,200"],
            "上  月": ["", "2,050", "8,120", "90.8%", "", "", "2,095", "19,820", "9,460", "", "", "404", "11,800", "3,050"],
            "环  比": ["", "↑1.8%", "↑1.5%", "↑0.5%", "", "", "↑2.1%", "↑2.6%", "↑0.4%", "", "", "↑11.4%", "↑4.2%", "↑4.9%"],
            "本年累计": ["", "12,380", "48,960", "—", "", "", "12,590", "119,450", "9,488", "", "", "—", "—", "—"],
            "年度计划": ["", "25,200", "98,000", "—", "", "", "25,200", "245,000", "9,500", "", "", "—", "—", "—"],
            "计划进度": ["", "49.1%", "50.0%", "—", "", "", "50.0%", "48.8%", "—", "", "", "—", "—", "—"],
        })

    elif "同比" in report_name or "环比" in report_name or "分析" in report_name or "底稿" in report_name:
        return pd.DataFrame({
            "分  析  项  目": [
                "营业收入", "营业成本", "毛利润", "毛利率",
                "管理费用", "财务费用", "净利润", "净利率",
                "资产总额", "负债合计", "资产负债率",
            ],
            f"本期（{unit}）": [
                "85,420", "62,180", "23,240", "27.2%",
                "3,680", "1,120", "14,297", "16.7%",
                "132,910", "57,650", "43.4%",
            ],
            f"对比期（{unit}）": [
                "78,930", "58,640", "20,290", "25.7%",
                "3,420", "980", "12,112", "15.3%",
                "127,020", "53,200", "41.9%",
            ],
            "变  动  幅  度": [
                "↑8.2%", "↑6.0%", "↑14.5%", "↑1.5pp",
                "↑7.6%", "↑14.3%", "↑18.0%", "↑1.4pp",
                "↑4.6%", "↑8.4%", "↑1.5pp",
            ],
            "综合评价": [
                "✅ 良好", "✅ 正常", "✅ 优秀", "✅ 改善",
                "✅ 正常", "⚠️ 关注", "✅ 优秀", "✅ 改善",
                "✅ 正常", "⚠️ 关注", "⚠️ 关注",
            ],
        })

    elif "成本" in report_name:
        return pd.DataFrame({
            "成本项目": [
                "一、直接材料", "  矿石原料", "  硫酸", "  电解液", "  其他辅料",
                "二、直接人工", "  工资", "  福利费",
                "三、制造费用", "  折旧", "  维修费", "  电力费", "  其他",
                "四、生产成本合计",
                "五、单位成本（美元/吨铜）",
            ],
            f"本月金额（{unit}）": [
                "", "5,820", "1,240", "340", "680",
                "", "420", "84",
                "", "2,860", "380", "3,700", "540",
                "16,064",
                "4,280（美元/吨）",
            ],
            "本月占比": [
                "", "36.2%", "7.7%", "2.1%", "4.2%",
                "", "2.6%", "0.5%",
                "", "17.8%", "2.4%", "23.0%", "3.4%",
                "100%",
                "—",
            ],
            f"上月金额（{unit}）": [
                "", "5,690", "1,210", "330", "660",
                "", "415", "83",
                "", "2,820", "370", "3,720", "520",
                "15,818",
                "4,310（美元/吨）",
            ],
            "环比变化": [
                "", "↑2.3%", "↑2.5%", "↑3.0%", "↑3.0%",
                "", "↑1.2%", "↑1.2%",
                "", "↑1.4%", "↑2.7%", "↓0.5%", "↑3.8%",
                "↑1.6%",
                "↓0.7%",
            ],
        })

    else:
        # 通用 / 科目余额表
        return pd.DataFrame({
            "科目编码": ["1001", "1002", "1012", "1122", "1221", "1231", "1401", "1501", "1502", "6001"],
            "科目名称": ["库存现金", "银行存款", "其他货币资金", "应收票据", "应收账款", "预付账款", "存货", "固定资产", "累计折旧", "主营业务收入"],
            f"期初余额（{unit}）": ["50", "10,180", "2,220", "—", "8,320", "1,450", "14,230", "92,100", "(2,640)", "—"],
            f"本期借方（{unit}）": ["2,400", "58,230", "—", "—", "12,450", "3,200", "8,900", "3,330", "780", "—"],
            f"本期贷方（{unit}）": ["2,380", "56,480", "—", "—", "12,050", "2,850", "7,570", "—", "—", "85,420"],
            f"期末余额（{unit}）": ["70", "11,930", "2,220", "—", "8,720", "1,800", "15,560", "95,430", "(3,420)", "85,420"],
        })


# ====================================================================
# 国企风格财务表格渲染
# ====================================================================
def render_finance_table(df: pd.DataFrame, report_name: str = "") -> str:
    """将 DataFrame 渲染为国企风格 HTML 财务表格"""

    SECTION_PREFIXES = ("一、", "二、", "三、", "四、", "五、", "六、", "七、", "八、", "九、", "十、")
    TOTAL_KEYWORDS   = ("合计", "总计", "净利润", "负债和所有者")

    def classify(first_val: str):
        s = str(first_val)
        stripped = s.strip()
        if not stripped:
            return "sep", 0
        indent = len(s) - len(stripped)
        level  = indent // 2
        # 顶级合计（无缩进且含关键词）
        if indent == 0 and any(k in stripped for k in TOTAL_KEYWORDS):
            return "grandtotal", 0
        # 一级分类标题
        if any(stripped.startswith(p) for p in SECTION_PREFIXES):
            return "section", 0
        # 子合计（有缩进 + 含合计）
        if indent > 0 and "合计" in stripped:
            return "subtotal", level
        return "normal", level

    def colorize(val: str) -> str:
        v = str(val)
        if v.startswith("↑"):
            return f'<span style="color:#16a34a;font-weight:600">{v}</span>'
        if v.startswith("↓"):
            return f'<span style="color:#dc2626;font-weight:600">{v}</span>'
        if v.startswith("✅"):
            return f'<span style="color:#16a34a">{v}</span>'
        if v.startswith("⚠️"):
            return f'<span style="color:#d97706">{v}</span>'
        return v

    cols = list(df.columns)

    # ── 表头 ──
    ths = "".join(
        f'<th style="text-align:{"left" if i == 0 else "right"}">{c}</th>'
        for i, c in enumerate(cols)
    )
    thead = f"<thead><tr>{ths}</tr></thead>"

    # ── 表体 ──
    rows_html = []
    odd = True
    for _, row in df.iterrows():
        first_val = str(row.iloc[0]) if len(row) > 0 else ""
        rtype, level = classify(first_val)

        if rtype == "sep":
            rows_html.append(f'<tr class="ft-sep"><td colspan="{len(cols)}"></td></tr>')
            continue

        indent_px = level * 16 + 10
        first_td = f'<td style="text-align:left;padding-left:{indent_px}px">{first_val.strip()}</td>'
        rest_tds  = "".join(
            f'<td style="text-align:right">{colorize(str(v))}</td>'
            for v in list(row)[1:]
        )

        if rtype == "normal":
            cls = f"ft-normal ft-{'odd' if odd else 'even'}"
            odd = not odd
        else:
            cls = f"ft-{rtype}"

        rows_html.append(f'<tr class="{cls}">{first_td}{rest_tds}</tr>')

    tbody = f"<tbody>{''.join(rows_html)}</tbody>"
    return f'<div class="ft-wrap"><table class="ft">{thead}{tbody}</table></div>'


# ====================================================================
# 重要指标快报 — 大卡片可视化
# ====================================================================
def render_quick_report(report_name: str, currency: str) -> str:
    """将重要指标快报渲染为 4 列大卡片网格，同比/环比突出显示"""
    u = "美元" if currency == "美元" else "元人民币"

    kpis = [
        {"name": "铜  产  量",  "val": "2,086",  "unit": "吨",
         "yoy": "+3.8%", "yoy_up": True,  "mom": "+1.8%", "mom_up": True,
         "plan": "2,100",  "rate": 99.3,  "yoy_base": "2,010"},
        {"name": "铜  销  量",  "val": "2,140",  "unit": "吨",
         "yoy": "+2.4%", "yoy_up": True,  "mom": "+2.1%", "mom_up": True,
         "plan": "2,100",  "rate": 101.9, "yoy_base": "2,090"},
        {"name": "综合回收率", "val": "91.3",   "unit": "%",
         "yoy": "+0.8%", "yoy_up": True,  "mom": "+0.5%", "mom_up": True,
         "plan": "91.0",   "rate": 100.3, "yoy_base": "90.6"},
        {"name": "生 产 成 本", "val": "4,280",  "unit": f"{u}/吨",
         "yoy": "−1.2%", "yoy_up": False, "mom": "−0.7%", "mom_up": False,
         "plan": "4,350",  "rate": 101.6, "yoy_base": "4,332"},
        {"name": "销 售 收 入", "val": "20,330", "unit": f"万{u}",
         "yoy": "+8.2%", "yoy_up": True,  "mom": "+2.6%", "mom_up": True,
         "plan": "20,000", "rate": 101.7, "yoy_base": "18,790"},
        {"name": "净  利  润",  "val": "1,430",  "unit": f"万{u}",
         "yoy": "+18.0%","yoy_up": True,  "mom": "+3.6%", "mom_up": True,
         "plan": "1,400",  "rate": 102.1, "yoy_base": "1,212"},
        {"name": "电       耗", "val": "1,850",  "unit": "度/吨铜",
         "yoy": "−2.1%", "yoy_up": False, "mom": "−1.1%", "mom_up": False,
         "plan": "1,900",  "rate": 102.6, "yoy_base": "1,890"},
        {"name": "员 工 人 数", "val": "486",    "unit": "人",
         "yoy": "+1.5%", "yoy_up": True,  "mom": "+0.2%", "mom_up": True,
         "plan": "490",    "rate": 99.2,  "yoy_base": "479"},
    ]

    cards = []
    for k in kpis:
        yoy_c = "#16a34a" if k["yoy_up"] else "#dc2626"
        mom_c = "#16a34a" if k["mom_up"] else "#dc2626"
        ya    = "▲" if k["yoy_up"] else "▼"
        ma    = "▲" if k["mom_up"] else "▼"
        bar_w = min(k["rate"], 100)
        bar_c = "#22c55e" if k["rate"] >= 100 else "#f59e0b" if k["rate"] >= 95 else "#ef4444"

        cards.append(f"""<div class="kd-card">
  <div class="kd-name">{k["name"]}</div>
  <div class="kd-val">{k["val"]}<span class="kd-unit"> {k["unit"]}</span></div>
  <div class="kd-yoy" style="color:{yoy_c}">{ya} {k["yoy"]} <span class="kd-yoy-label">同比</span></div>
  <div class="kd-divider"></div>
  <div class="kd-row2">
    <span style="color:{mom_c};font-weight:600">{ma} {k["mom"]} 环比</span>
    <span class="kd-plan-val">计划 {k["plan"]}</span>
  </div>
  <div class="kd-prog-wrap"><div class="kd-prog-bar" style="width:{bar_w}%;background:{bar_c}"></div></div>
  <div class="kd-rate-row">
    <span style="color:{bar_c};font-weight:600">完成率 {k["rate"]}%</span>
    <span class="kd-yoy-base">上年同期 {k["yoy_base"]}</span>
  </div>
</div>""")

    return f'<div class="kd-grid">{"".join(cards)}</div>'


# ====================================================================
# AI 响应
# ====================================================================
def ai_respond(query: str, report_name: str, period: str, currency: str) -> str:
    gemini_key = get_api_key("GEMINI_API_KEY")
    if gemini_key:
        try:
            import google.generativeai as genai
            genai.configure(api_key=gemini_key)
            model = genai.GenerativeModel("gemini-2.0-flash-exp")
            prompt = (
                f"你是一个央企财务分析助手。当前报表：{report_name}，"
                f"期间：{period_label(period)}，币种：{currency}。"
                f"用户指令：{query}\n"
                "请用简洁专业中文回答（2~3句），若涉及取数规则给出科目编码和字段名。"
            )
            resp = model.generate_content(prompt)
            return resp.text.strip()
        except Exception:
            pass

    # 规则引擎演示
    q = query
    if "收入" in q or "6001" in q:
        return f"主营业务收入取自科目余额表科目编码 6001，取「本期贷方发生额」。{period_label(period)}实现营业收入 **85,420 万{currency}**，同比增长 8.2%，主要来自铜精矿销售。"
    elif "成本" in q or "6401" in q:
        return f"营业成本取自科目余额表科目编码 6401，取「本期借方发生额」。本期成本 **62,180 万{currency}**，毛利率 27.2%，较上期提升 1.5pp，成本管控成效显著。"
    elif "净利" in q or "利润" in q:
        return f"本期净利润 **14,297 万{currency}**，同比增长 18.0%。主因铜价上涨驱动收入增长 8.2%，同时生产降本使毛利率提升至 27.2%，盈利能力持续改善。"
    elif "铜" in q or "产量" in q:
        return f"本月铜产量 **2,086 吨**，完成计划 99.3%；综合回收率 91.3%，优于 91.0% 的计划目标；年度累计 12,380 吨，完成年度计划 49.1%，进度正常。"
    elif "分析" in q or "变动" in q or "原因" in q:
        return f"{report_name}显示本期经营情况良好：收入增长 8.2%，净利润增长 18.0%，盈利能力持续提升。建议关注财务费用同比上涨 14.3%，可优化融资结构降低资金成本。"
    else:
        return f"已接收指令：「{query[:50]}」。系统将从基础资料库（科目余额表、成本表）自动匹配取数规则并填入 **{report_name}** 对应单元格。如需指定科目编码，请在指令中注明。"


# ====================================================================
# CSS — 央企级现代界面
# ====================================================================
st.markdown("""<style>
html, body, [data-testid="stApp"] {
    font-family: "PingFang SC","Microsoft YaHei","Helvetica Neue",sans-serif;
    background: #edf1f8;
}
#MainMenu, footer { visibility: hidden; }
.stDeployButton { display: none !important; }
[data-testid="stHeader"] { background: transparent !important; }
.block-container { padding-top: 0.6rem !important; padding-bottom: 1rem !important; }

/* ── 顶部标题栏 ── */
.app-header {
    background: linear-gradient(105deg, #081c38 0%, #0c3060 45%, #1255a8 100%);
    color: #fff;
    border-radius: 8px;
    padding: 0.8rem 1.4rem;
    display: flex; justify-content: space-between; align-items: center;
    box-shadow: 0 4px 16px rgba(8,28,56,0.30);
    margin-bottom: 0.5rem;
}
.app-header h2 { margin: 0; font-size: 1.08rem; font-weight: 700; letter-spacing: 0.05em; }
.app-header .sub { font-size: 0.70rem; opacity: 0.60; margin-top: 3px; letter-spacing: 0.1em; }
.app-header .right { text-align: right; font-size: 0.78rem; line-height: 1.8; opacity: 0.85; }
.ai-dot { color: #4ade80; font-weight: 800; }

/* ── 模块标签页 ── */
.stTabs [data-baseweb="tab-list"] {
    background: #fff;
    border-bottom: 2px solid #cdd8eb;
    border-radius: 6px 6px 0 0;
    padding: 0 0.4rem; gap: 0;
    box-shadow: 0 1px 5px rgba(0,0,0,0.07);
}
.stTabs [data-baseweb="tab"] {
    font-size: 0.87rem; font-weight: 500; color: #4a5870;
    padding: 0.58rem 1.15rem; border-radius: 0;
}
.stTabs [aria-selected="true"] {
    color: #0c3060 !important; font-weight: 700 !important;
    border-bottom: 3px solid #1255a8 !important;
}
.stTabs [data-baseweb="tab-panel"] {
    background: #fff;
    border: 1px solid #cdd8eb; border-top: none;
    border-radius: 0 0 8px 8px;
    padding: 0.9rem 1rem;
    box-shadow: 0 2px 8px rgba(0,0,0,0.05);
}

/* ── 左侧报表导航 — 自定义 LED 指示灯 ── */
div[data-testid="stRadio"] > div { gap: 0 !important; }

/* 隐藏 BaseWeb 原生圆圈区域 */
div[data-testid="stRadio"] label > div:first-child { display: none !important; }

/* 每一行：flex 横排，圆点 + 文字对齐 */
div[data-testid="stRadio"] label {
    display: flex !important; align-items: center !important;
    padding: 6px 10px 6px 8px !important; border-radius: 4px !important;
    font-size: 0.83rem !important; color: #4a5870 !important;
    margin-bottom: 2px !important; border-left: 3px solid transparent !important;
    cursor: pointer !important; transition: background 0.15s, color 0.15s !important;
}

/* 指示灯圆点（默认灰色）*/
div[data-testid="stRadio"] label::before {
    content: "" !important;
    display: inline-block !important; flex-shrink: 0 !important;
    width: 7px !important; height: 7px !important; border-radius: 50% !important;
    background: #c0cfe0 !important; margin-right: 9px !important;
    transition: background 0.2s, box-shadow 0.2s !important;
}

/* 悬浮 */
div[data-testid="stRadio"] label:hover { background: #e8f0fa !important; color: #0c3060 !important; }
div[data-testid="stRadio"] label:hover::before { background: #90b8e0 !important; }

/* 选中 — 蓝色左边框 + 浅蓝背景 + 绿色 LED 灯亮起 */
div[data-testid="stRadio"] label:has(input:checked),
div[data-testid="stRadio"] label[data-checked="true"] {
    background: #dbeafe !important; border-left-color: #1255a8 !important;
    color: #0c3060 !important; font-weight: 600 !important;
}
div[data-testid="stRadio"] label:has(input:checked)::before,
div[data-testid="stRadio"] label[data-checked="true"]::before {
    background: #22c55e !important;
    box-shadow: 0 0 0 2px rgba(34,197,94,0.20), 0 0 7px rgba(34,197,94,0.55) !important;
}

/* ── 报表标题区 ── */
.rpt-header {
    display: flex; justify-content: space-between; align-items: center;
    margin-bottom: 0.65rem; padding-bottom: 0.55rem;
    border-bottom: 2px solid #e6eef8;
}
.rpt-title { font-size: 0.96rem; font-weight: 700; color: #081c38; }
.rpt-meta { font-size: 0.72rem; color: #8090a8; }
.badge { display: inline-block; border-radius: 3px; padding: 2px 8px; font-size: 0.70rem; font-weight: 600; }
.badge-ok   { background: #d1fae5; color: #065f46; }
.badge-wait { background: #fef3c7; color: #92400e; }

/* ── KPI 卡片 ── */
.kpi-grid { display: flex; gap: 0.65rem; margin: 0.75rem 0 0.9rem; }
.kpi-card {
    flex: 1; background: #fff; border: 1px solid #cdd8eb;
    border-top: 3px solid #1255a8; border-radius: 6px;
    padding: 0.65rem 0.85rem; text-align: center;
    box-shadow: 0 1px 4px rgba(0,0,0,0.05);
}
.kpi-v { font-size: 1.45rem; font-weight: 700; color: #081c38; line-height: 1.2; }
.kpi-l { font-size: 0.70rem; color: #6b7a8d; margin-top: 2px; }
.kpi-c { font-size: 0.70rem; margin-top: 4px; }
.up { color: #059669; } .dn { color: #dc2626; }

/* ── 工具栏区 ── */
.toolbar-wrap {
    background: #f3f7fd; border: 1px solid #cdd8eb; border-radius: 6px;
    padding: 0.42rem 0.75rem; margin-bottom: 0.75rem;
    display: flex; align-items: center; gap: 4px;
}
.toolbar-label { font-size: 0.78rem; font-weight: 600; color: #4a5870; margin-right: 4px; white-space: nowrap; }
.toolbar-sep { width: 1px; height: 18px; background: #c0cfdf; margin: 0 6px; flex-shrink: 0; }

/* ── AI 对话区 ── */
.ai-zone { background: #f0f6ff; border: 1px solid #b8d0f0; border-radius: 8px; padding: 0.65rem 0.9rem; margin-top: 0.9rem; }
.ai-label { font-size: 0.73rem; font-weight: 600; color: #4a6890; margin-bottom: 5px; }
.ai-reply {
    background: #fff; border-left: 3px solid #1255a8;
    border-radius: 0 5px 5px 0; padding: 0.55rem 0.8rem;
    font-size: 0.83rem; color: #2c3e50; line-height: 1.7; margin-top: 0.5rem;
}

/* ── 原生 DataFame（资料库预览用）── */
[data-testid="stDataFrame"] { border: 1px solid #cdd8eb !important; border-radius: 5px !important; overflow: hidden !important; }

/* ── 国企风格财务报表表格 ── */
.ft-wrap {
    overflow-x: auto; border-radius: 6px;
    border: 1px solid #9eb8d8;
    box-shadow: 0 3px 12px rgba(8,28,56,0.10); margin-top: 0.4rem;
}
.ft {
    width: 100%; border-collapse: collapse;
    font-size: 0.82rem;
    font-family: "PingFang SC","Microsoft YaHei","Helvetica Neue",sans-serif;
}
/* 表头 — 深海蓝 */
.ft thead th {
    background: #0c3060; color: #ddeeff;
    padding: 9px 13px; font-weight: 600; font-size: 0.79rem;
    border-right: 1px solid rgba(255,255,255,0.10);
    white-space: nowrap; letter-spacing: 0.04em;
}
.ft thead th:last-child { border-right: none; }
/* 普通数据行 */
.ft tbody td {
    padding: 6px 13px; border-bottom: 1px solid #e4ecf5;
    color: #243040; vertical-align: middle;
}
.ft tbody tr.ft-odd  td { background: #fff; }
.ft tbody tr.ft-even td { background: #f5f8fd; }
.ft tbody tr.ft-normal:hover td { background: #e4f0ff !important; }
/* 一级分类行 — 矿蓝浅底 */
.ft tbody tr.ft-section td {
    background: #d6e9ff !important; color: #0c3060 !important;
    font-weight: 700; font-size: 0.83rem;
    border-top: 1px solid #93c5fd; border-bottom: 1px solid #93c5fd;
}
/* 子合计行 — 极浅蓝 + 斜体 */
.ft tbody tr.ft-subtotal td {
    background: #eef5ff !important; color: #1e4a8a !important;
    font-weight: 600; border-bottom: 1px solid #c3d9f5;
}
/* 顶级合计/总计 — 深蓝白字 */
.ft tbody tr.ft-grandtotal td {
    background: #1255a8 !important; color: #fff !important;
    font-weight: 700; font-size: 0.84rem;
    border-top: 2px solid #0c3060; border-bottom: 2px solid #0c3060;
}
/* 空行分隔 */
.ft tbody tr.ft-sep td { height: 5px; background: #e4ecf5; padding: 0; border: none; }

/* ── 重要指标快报 — 大卡片网格 ── */
.kd-grid {
    display: grid; grid-template-columns: repeat(4, 1fr);
    gap: 12px; margin: 6px 0 14px;
}
.kd-card {
    background: #fff; border: 1px solid #c0d4ea; border-top: 3px solid #1255a8;
    border-radius: 7px; padding: 13px 14px 11px;
    box-shadow: 0 2px 8px rgba(8,28,56,0.07);
}
.kd-name { font-size: 0.72rem; color: #6b7a8d; font-weight: 500; letter-spacing: 0.04em; margin-bottom: 5px; }
.kd-val  { font-size: 1.55rem; font-weight: 700; color: #081c38; line-height: 1.15; margin-bottom: 5px; }
.kd-unit { font-size: 0.70rem; color: #8090a8; font-weight: 400; }
.kd-yoy  { font-size: 1.05rem; font-weight: 700; margin-bottom: 2px; }
.kd-yoy-label { font-size: 0.70rem; font-weight: 500; opacity: 0.75; }
.kd-divider { height: 1px; background: #e8eef6; margin: 7px 0; }
.kd-row2 { display: flex; justify-content: space-between; font-size: 0.72rem; margin-bottom: 7px; }
.kd-plan-val { color: #8090a8; }
.kd-prog-wrap { background: #e8eef6; border-radius: 3px; height: 5px; margin-bottom: 5px; overflow: hidden; }
.kd-prog-bar  { height: 100%; border-radius: 3px; }
.kd-rate-row  { display: flex; justify-content: space-between; font-size: 0.70rem; }
.kd-yoy-base  { color: #8090a8; }

/* ── 侧边栏 ── */
[data-testid="stSidebar"] { background: #f3f7fd !important; border-right: 1px solid #cdd8eb !important; }

/* 滚动条 */
::-webkit-scrollbar { width: 5px; height: 5px; }
::-webkit-scrollbar-track { background: #edf1f8; }
::-webkit-scrollbar-thumb { background: #b0c4d8; border-radius: 3px; }
</style>""", unsafe_allow_html=True)


# ====================================================================
# Session State 初始化
# ====================================================================
def _ss(key, default):
    if key not in st.session_state:
        st.session_state[key] = default


_ss("currency", "美元")
_ss("computed_reports", set())
_ss("ai_responses", {})
_ss("word_generated", False)


# ====================================================================
# 全局数据查询
# ====================================================================
period_opts = period_options()

conn = get_db()
upload_count   = conn.execute("SELECT COUNT(*) FROM uploads").fetchone()[0]
gen_count      = conn.execute("SELECT COUNT(*) FROM generations").fetchone()[0]
conn.close()


# ====================================================================
# 顶部标题 + 全局控制行
# ====================================================================
hc1, hc2, hc3 = st.columns([4, 1.1, 1.1])

with hc1:
    st.markdown("""
    <div class="app-header">
        <div>
            <h2>📊 财务报表智能生成平台</h2>
            <div class="sub">中色华鑫马本德矿业有限公司 &nbsp;·&nbsp; FINANCIAL INTELLIGENCE PLATFORM</div>
        </div>
        <div class="right">
            <span class="ai-dot">●</span> AI 引擎就绪<br>
            <span style="opacity:0.55">演示版 v0.3</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

with hc2:
    selected_period = st.selectbox(
        "期间", period_opts, format_func=period_label, index=0,
        label_visibility="visible",
    )

with hc3:
    currency = st.radio(
        "币种", ["美元", "人民币"],
        horizontal=True,
        index=0 if st.session_state.currency == "美元" else 1,
        label_visibility="visible",
    )
    st.session_state.currency = currency


# ====================================================================
# 侧边栏 — 数据上传 + 统计
# ====================================================================
with st.sidebar:
    st.markdown("### 📤 上传数据文件")
    st.caption(f"当前期间：**{period_label(selected_period)}**")
    st.caption("支持 NC 系统导出的 Excel / CSV 文件")

    uploaded_file = st.file_uploader(
        "选择文件", type=["xlsx", "xls", "csv"],
        label_visibility="collapsed",
    )
    if uploaded_file:
        try:
            file_bytes = uploaded_file.read()
            file_ext   = uploaded_file.name.rsplit(".", 1)[-1].lower()
            df_dict    = read_excel_file(file_bytes, file_ext)
            total_rows = sum(len(df) for df in df_dict.values())
            st.caption(f"✅ {len(df_dict)} 个Sheet，共 {total_rows:,} 行")
            if st.button("💾 保存到平台", type="primary", use_container_width=True):
                save_path = period_data_dir(selected_period) / uploaded_file.name
                with open(save_path, "wb") as f:
                    f.write(file_bytes)
                conn = get_db()
                cur = conn.execute(
                    "INSERT INTO uploads (period,filename,file_type,sheet_count,row_count,upload_time,file_path) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (selected_period, uploaded_file.name, file_ext,
                     len(df_dict), total_rows, datetime.now().isoformat(), str(save_path)),
                )
                if len(file_bytes) < 10 * 1024 * 1024:
                    st.session_state[f"fc_{cur.lastrowid}"] = file_bytes
                conn.commit(); conn.close()
                st.success("已保存！")
                st.rerun()
        except Exception as e:
            st.error(f"解析失败：{e}")

    st.markdown("---")
    st.markdown("### 📁 本期文件")
    conn = get_db()
    period_uploads = conn.execute(
        "SELECT id, filename FROM uploads WHERE period=? ORDER BY upload_time DESC",
        (selected_period,),
    ).fetchall()
    conn.close()
    if period_uploads:
        for u in period_uploads:
            st.caption(f"📄 {u[1]}")
    else:
        st.caption("暂无文件，请上传")

    st.markdown("---")
    st.markdown("### 📊 平台统计")
    st.caption(f"已上传文件：**{upload_count}** 个")
    st.caption(f"已生成报表：**{gen_count}** 份")
    st.markdown("---")
    st.caption(f"v0.3  ·  {datetime.now().strftime('%Y-%m-%d')}")


# ====================================================================
# 主界面 — 模块标签页
# ====================================================================
module_names = list(REPORT_MODULES.keys())
tabs = st.tabs(module_names)

for mod_name, tab in zip(module_names, tabs):
    with tab:

        # ── 基础资料库 ──────────────────────────────────────────────
        if mod_name == "🗄️ 基础资料库":
            conn = get_db()
            all_files = conn.execute(
                "SELECT id, period, filename, sheet_count, row_count, file_path "
                "FROM uploads ORDER BY period DESC, upload_time DESC"
            ).fetchall()
            conn.close()

            lc, rc = st.columns([1.4, 4.6])
            with lc:
                st.markdown(
                    '<p style="font-size:0.68rem;font-weight:700;color:#8090a8;'
                    'letter-spacing:0.1em;text-transform:uppercase;margin:4px 0 6px 4px;">'
                    'UPLOADED FILES</p>',
                    unsafe_allow_html=True,
                )
                if not all_files:
                    st.caption("暂无文件，请在侧边栏上传")
                    sel_file = None
                else:
                    file_labels = [
                        f"【{period_label(f[1]) if f[1] else '—'}】 {f[2]}"
                        for f in all_files
                    ]
                    sel_idx = st.radio(
                        "files", range(len(all_files)),
                        format_func=lambda i: file_labels[i],
                        key="lib_sel", label_visibility="collapsed",
                    )
                    sel_file = all_files[sel_idx]

            with rc:
                if not all_files:
                    st.info("请先在左侧侧边栏上传 NC 系统导出的数据文件（科目余额表、成本表等）。")
                else:
                    fpath, fname = sel_file[5], sel_file[2]
                    cache_key = f"fc_{sel_file[0]}"

                    st.markdown(
                        f'<div class="rpt-header">'
                        f'<div class="rpt-title">📂 {fname}</div>'
                        f'<div class="rpt-meta">期间：{period_label(sel_file[1])} · '
                        f'{sel_file[3]} 个Sheet · {sel_file[4]:,} 行</div>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )

                    df_dict = None
                    if fpath and os.path.exists(fpath):
                        try:
                            ext = fpath.rsplit(".", 1)[-1].lower()
                            df_dict = read_excel_file(fpath, ext)
                        except Exception:
                            pass
                    elif cache_key in st.session_state:
                        try:
                            ext = fname.rsplit(".", 1)[-1].lower()
                            df_dict = read_excel_file(st.session_state[cache_key], ext)
                        except Exception:
                            pass

                    if df_dict:
                        sheet_names = list(df_dict.keys())
                        if len(sheet_names) == 1:
                            st.dataframe(
                                prepare_for_display(df_dict[sheet_names[0]]),
                                use_container_width=True, height=480, hide_index=True,
                            )
                        else:
                            ptabs = st.tabs([f"📄 {sn}" for sn in sheet_names[:12]])
                            for ptab, sn in zip(ptabs, sheet_names[:12]):
                                with ptab:
                                    df = df_dict[sn]
                                    st.caption(f"{len(df):,} 行 × {len(df.columns)} 列")
                                    st.dataframe(
                                        prepare_for_display(df),
                                        use_container_width=True, height=440, hide_index=True,
                                    )
                    else:
                        st.warning("⚠️ 文件不可读（云端会话缓存已过期），请重新上传。")

        # ── Word 报告 ────────────────────────────────────────────────
        elif mod_name == "📝 Word报告":
            lc, rc = st.columns([1.4, 4.6])
            with lc:
                st.markdown(
                    '<p style="font-size:0.68rem;font-weight:700;color:#8090a8;'
                    'letter-spacing:0.1em;text-transform:uppercase;margin:4px 0 6px 4px;">'
                    'REPORT LIST</p>',
                    unsafe_allow_html=True,
                )
                st.radio(
                    "word_rpts", ["月度财务分析报告（完整版）"],
                    key="word_rpt_sel", label_visibility="collapsed",
                )
            with rc:
                # 工具栏
                t1, t2, t3, t4 = st.columns([1.5, 1.5, 1.5, 3.5])
                with t1:
                    if st.button("🤖 AI生成报告", type="primary",
                                  use_container_width=True, key="gen_word_btn"):
                        with st.spinner("AI 正在撰写月度财务分析报告…"):
                            time.sleep(2.5)
                        st.session_state.word_generated = True
                        st.rerun()
                with t2:
                    st.download_button(
                        "📥 下载 Word",
                        data=b"demo placeholder",
                        file_name=f"月度财务分析报告_{period_label(selected_period)}.docx",
                        key="dl_word", use_container_width=True,
                    )
                with t3:
                    st.download_button(
                        "📥 下载 PDF",
                        data=b"demo placeholder",
                        file_name=f"月度财务分析报告_{period_label(selected_period)}.pdf",
                        key="dl_word_pdf", use_container_width=True,
                    )

                badge = '<span class="badge badge-ok">已生成</span>' if st.session_state.word_generated else '<span class="badge badge-wait">待生成</span>'
                st.markdown(
                    f'<div class="rpt-header">'
                    f'<div class="rpt-title">中色华鑫马本德矿业有限公司<br>'
                    f'{period_label(selected_period)}月度财务分析报告</div>'
                    f'<div class="rpt-meta">AI智能生成 · 含5张数据表格 · {badge}</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

                if st.session_state.word_generated:
                    st.markdown("""
<div class="ai-reply">
<b>一、总体经营情况</b><br>
2026年1月，公司生产经营总体平稳，实现营业收入 <b>85,420 万美元</b>，同比增长 8.2%；
净利润 <b>14,297 万美元</b>，同比增长 18.0%，盈利能力持续提升。
铜产量 2,086 吨，完成计划 99.3%。<br><br>

<b>二、主要指标完成情况</b><br>
综合回收率 91.3%，优于 91.0% 的计划目标；
生产成本 4,280 美元/吨，较计划降低 1.6%，成本管控成效显著。
年度累计铜产量 12,380 吨，完成年度计划 49.1%，进度正常。<br><br>

<b>三、财务状况分析</b><br>
资产总计 132,910 万美元，较期初增长 4.6%；资产负债率 43.4%，处于合理区间；
货币资金 12,450 万美元，流动性充足，短期偿债能力较强。<br><br>

<b>四、存在问题与建议</b><br>
财务费用同比上涨 14.3%，建议优化融资结构，适时置换高息负债；
应收账款较期初增加 5.5%，需加强账款回收管理，防范坏账风险。<br><br>

<i style="color:#8090a8;font-size:0.80rem;">
（以上为AI演示生成内容，正式报告将基于 NC 系统实际数据自动生成，含完整数据表格及图表）
</i>
</div>""", unsafe_allow_html=True)
                else:
                    st.info(
                        "点击「AI生成报告」，系统将基于本期财务数据自动撰写完整月度财务分析报告，"
                        "包含：总体经营情况 · 主要指标完成情况 · 财务状况分析 · "
                        "成本费用分析 · 存在问题与建议。支持中英双语，含 5 张数据表格。"
                    )

        # ── 普通报表模块（月度快报 / 基础报表 / 分析底稿）──────────
        else:
            reports = REPORT_MODULES[mod_name]
            if not reports:
                st.info("该模块暂无报表配置。")
                continue

            # 模块 key（去掉 emoji）
            mod_key = mod_name.split(" ", 1)[-1].replace(" ", "_")
            nav_key = f"nav_{mod_key}"
            _ss(nav_key, reports[0])

            lc, rc = st.columns([1.4, 4.6])

            # ── 左侧导航 ──
            with lc:
                st.markdown(
                    '<p style="font-size:0.68rem;font-weight:700;color:#8090a8;'
                    'letter-spacing:0.1em;text-transform:uppercase;margin:4px 0 6px 4px;">'
                    'REPORT LIST</p>',
                    unsafe_allow_html=True,
                )
                selected_rpt = st.radio(
                    "rpt_nav", reports,
                    key=nav_key, label_visibility="collapsed",
                )

            # ── 右侧内容区 ──
            with rc:
                r_key = f"{mod_key}__{selected_rpt}"
                is_computed = r_key in st.session_state.computed_reports

                # 工具栏
                tc1, tc2, tc3, tsp1, tc4, tc5 = st.columns([1.3, 1.7, 1.9, 0.2, 1.3, 1.3])
                with tc1:
                    run_single = st.button(
                        "▶ 运算当前", key=f"rs_{r_key}", use_container_width=True, type="primary",
                    )
                with tc2:
                    run_cat = st.button(
                        "▶▶ 运算本类报表", key=f"rc_{r_key}", use_container_width=True,
                    )
                with tc3:
                    run_all = st.button(
                        "▶▶▶ 全部报表运算", key=f"ra_{r_key}", use_container_width=True,
                    )
                with tc4:
                    st.download_button(
                        "📥 导出 Excel",
                        data=b"demo",
                        file_name=f"{selected_rpt}_{period_label(selected_period)}.xlsx",
                        key=f"dl_{r_key}", use_container_width=True,
                    )
                with tc5:
                    if st.button("📋 审核校验", key=f"audit_{r_key}", use_container_width=True):
                        st.toast("✅ 审核完成，数据无异常", icon="✅")

                # 运算逻辑
                if run_single:
                    with st.spinner(f"正在运算：{selected_rpt}…"):
                        time.sleep(1.2)
                    st.session_state.computed_reports.add(r_key)
                    st.rerun()
                if run_cat:
                    with st.spinner(f"正在运算本类全部报表…"):
                        time.sleep(2.2)
                    for rpt in reports:
                        st.session_state.computed_reports.add(f"{mod_key}__{rpt}")
                    st.success(f"✅ {mod_name.split(' ',1)[-1]} 全部 {len(reports)} 张报表运算完成")
                if run_all:
                    with st.spinner("正在运算所有模块全部报表…"):
                        time.sleep(3.5)
                    for mn, rlist in REPORT_MODULES.items():
                        mk = mn.split(" ", 1)[-1].replace(" ", "_")
                        for rpt in rlist:
                            st.session_state.computed_reports.add(f"{mk}__{rpt}")
                    st.success("✅ 全部报表运算完成！")

                # 报表标题
                badge_html = (
                    '<span class="badge badge-ok">已运算</span>'
                    if is_computed else
                    '<span class="badge badge-wait">待运算</span>'
                )
                st.markdown(
                    f'<div class="rpt-header">'
                    f'<div class="rpt-title">{selected_rpt}</div>'
                    f'<div class="rpt-meta">'
                    f'{period_label(selected_period)} &nbsp;·&nbsp; '
                    f'单位：万{"美元" if currency == "美元" else "元人民币"} &nbsp;·&nbsp; '
                    f'{badge_html}</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

                # 报表数据
                # 重要指标快报 → 大卡片网格（同比/环比突出显示）
                if "指标" in selected_rpt and "快报" in selected_rpt:
                    st.markdown(render_quick_report(selected_rpt, currency), unsafe_allow_html=True)
                else:
                    # 其他快报/分析类：顶部 4 个摘要 KPI 卡片
                    kpi_keywords = ["快报", "同比", "环比", "分析", "底稿"]
                    if any(k in selected_rpt for k in kpi_keywords):
                        ustr = "美元" if currency == "美元" else "元"
                        st.markdown(f"""
<div class="kpi-grid">
  <div class="kpi-card"><div class="kpi-v">14,297</div><div class="kpi-l">净利润（万{ustr}）</div><div class="kpi-c up">↑18.0% 同比</div></div>
  <div class="kpi-card"><div class="kpi-v">85,420</div><div class="kpi-l">营业收入（万{ustr}）</div><div class="kpi-c up">↑8.2% 同比</div></div>
  <div class="kpi-card"><div class="kpi-v">27.2%</div><div class="kpi-l">毛  利  率</div><div class="kpi-c up">↑1.5pp 同比</div></div>
  <div class="kpi-card"><div class="kpi-v">2,086t</div><div class="kpi-l">铜产量（本月）</div><div class="kpi-c up">↑1.8% 环比</div></div>
</div>""", unsafe_allow_html=True)
                    df = gen_demo_df(selected_rpt, currency)
                    st.markdown(render_finance_table(df, selected_rpt), unsafe_allow_html=True)

                # AI 取数对话
                st.markdown(
                    '<div class="ai-label">🤖 AI 智能取数 &nbsp;—&nbsp; '
                    '描述数据来源或提问，AI 自动匹配科目取数规则</div>',
                    unsafe_allow_html=True,
                )
                ai_c1, ai_c2 = st.columns([5.5, 0.8])
                with ai_c1:
                    ai_input = st.text_input(
                        "ai", key=f"ai_in_{r_key}",
                        placeholder='例："主营业务收入 取自科目余额表 6001 贷方发生额"  或  "分析本月净利润变动原因"',
                        label_visibility="collapsed",
                    )
                with ai_c2:
                    ai_send = st.button("发送", key=f"ai_btn_{r_key}",
                                         use_container_width=True, type="primary")

                if ai_send and ai_input.strip():
                    with st.spinner("AI 处理中…"):
                        resp = ai_respond(ai_input.strip(), selected_rpt, selected_period, currency)
                    st.session_state.ai_responses[r_key] = resp

                if r_key in st.session_state.ai_responses:
                    st.markdown(
                        f'<div class="ai-reply">🤖 {st.session_state.ai_responses[r_key]}</div>',
                        unsafe_allow_html=True,
                    )
