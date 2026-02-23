"""
财务报表智能生成平台 — Demo v0.2
================================
面向中色华鑫马本德矿业有限公司的月度财务报表自动化演示平台
"""

import streamlit as st
import pandas as pd
import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
import io
import time


# ── 页面配置（必须最先调用）──
st.set_page_config(
    page_title="财务报表智能生成平台",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ====================================================================
# 工具函数
# ====================================================================
def get_api_key(key_name: str) -> str | None:
    """安全读取 API Key — 优先级：Streamlit Secrets > 环境变量 > .env 文件"""
    try:
        return st.secrets[key_name]
    except (KeyError, FileNotFoundError):
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
    """格式化单元格：数字加千分符，空值变空字符串"""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    if isinstance(x, (int, float)):
        if float(x) == int(x):
            return f"{int(x):,d}"
        return f"{x:,.2f}"
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return ""
        try:
            num = float(s)
            if num == int(num):
                return f"{int(num):,d}"
            return f"{num:,.2f}"
        except (ValueError, OverflowError):
            pass
    return str(x)


def prepare_for_display(df):
    """准备DataFrame用于展示：处理合并单元格、Unnamed列名、千分符"""
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
    """生成最近24个月的期间选项，格式 YYYY-MM"""
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
    """统一读取Excel/CSV，返回 {sheet_name: DataFrame}"""
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
BASE_DIR    = Path(__file__).parent
DATA_DIR    = BASE_DIR / "data"
OUTPUT_DIR  = BASE_DIR / "output"
MAPPING_DIR = BASE_DIR / "mappings"
TEMPLATE_DIR = BASE_DIR / "templates"
DB_PATH     = DATA_DIR / "platform.db"

for d in [DATA_DIR, OUTPUT_DIR, MAPPING_DIR, TEMPLATE_DIR]:
    d.mkdir(exist_ok=True)


def init_db():
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS uploads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            period TEXT NOT NULL DEFAULT '',
            filename TEXT NOT NULL,
            file_type TEXT,
            sheet_count INTEGER DEFAULT 0,
            row_count INTEGER DEFAULT 0,
            upload_time TEXT NOT NULL,
            file_path TEXT,
            status TEXT DEFAULT '已上传'
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS mappings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            source_file TEXT,
            target_template TEXT,
            rules_json TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS generations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            period TEXT NOT NULL DEFAULT '',
            source_upload_id INTEGER,
            mapping_id INTEGER,
            ai_model TEXT,
            output_filename TEXT,
            output_path TEXT,
            status TEXT DEFAULT '生成中',
            created_at TEXT NOT NULL,
            duration_seconds REAL
        )
    """)
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
# CSS — 企业级深蓝风格
# ====================================================================
st.markdown("""
<style>
/* ─── 整体字体 ─── */
html, body, [data-testid="stApp"] {
    font-family: "PingFang SC", "Microsoft YaHei", "Helvetica Neue", sans-serif;
}

/* ─── 顶部标题栏 ─── */
.platform-header {
    background: linear-gradient(135deg, #1a3a5c 0%, #2563a8 60%, #1a6ba8 100%);
    color: white;
    padding: 0.9rem 1.4rem;
    border-radius: 8px;
    margin-bottom: 1.2rem;
    display: flex;
    justify-content: space-between;
    align-items: center;
    box-shadow: 0 2px 8px rgba(30,80,140,0.15);
}
.platform-header h1 {
    margin: 0;
    font-size: 1.3rem;
    font-weight: 700;
    letter-spacing: 0.04em;
}
.platform-header .sub {
    font-size: 0.82rem;
    opacity: 0.82;
    margin-top: 3px;
}
.platform-header .right {
    text-align: right;
    font-size: 0.82rem;
    opacity: 0.88;
    line-height: 1.6;
}

/* ─── 指标卡片 ─── */
.metric-card {
    background: white;
    border: 1px solid #dce8f5;
    border-top: 3px solid #2563a8;
    border-radius: 8px;
    padding: 0.9rem 1rem;
    text-align: center;
    box-shadow: 0 1px 4px rgba(0,0,0,0.05);
}
.metric-card .val {
    font-size: 1.75rem;
    font-weight: 700;
    color: #1a3a5c;
    line-height: 1.2;
}
.metric-card .lbl {
    font-size: 0.78rem;
    color: #666;
    margin-top: 3px;
}

/* ─── 流程步骤 ─── */
.step-box {
    background: #f4f8fd;
    border: 1px solid #c8ddf0;
    border-radius: 8px;
    padding: 0.8rem 0.6rem;
    text-align: center;
}
.step-num {
    background: #2563a8;
    color: white;
    width: 26px;
    height: 26px;
    border-radius: 50%;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    font-weight: 700;
    font-size: 0.85rem;
    margin-bottom: 5px;
}
.step-title { font-weight: 600; font-size: 0.88rem; color: #1a3a5c; }
.step-desc  { font-size: 0.73rem; color: #555; margin-top: 3px; }

/* ─── 报表类型列表 ─── */
.report-list {
    background: #f8fbff;
    border: 1px solid #d8e8f5;
    border-radius: 8px;
    padding: 0.9rem 1.1rem;
    font-size: 0.87rem;
    line-height: 1.8;
}
.report-list h4 { color: #1a3a5c; margin: 0 0 6px 0; font-size: 0.92rem; }

/* ─── 侧边栏 ─── */
[data-testid="stSidebar"] {
    background-color: #f6faff;
    border-right: 1px solid #d8e8f5;
}
[data-testid="stSidebar"] .stCaption { color: #444; }

/* ─── 数据表格 ─── */
[data-testid="stDataFrame"] {
    border: 1px solid #d0dde8;
    border-radius: 6px;
}
</style>
""", unsafe_allow_html=True)


# ====================================================================
# 侧边栏 — 期间选择 & 统计（全局）
# ====================================================================
with st.sidebar:
    st.markdown("### 📅 财务期间")
    period_opts = period_options()
    selected_period = st.selectbox(
        "期间",
        options=period_opts,
        format_func=period_label,
        index=0,
        label_visibility="collapsed",
    )
    st.caption(f"当前：**{period_label(selected_period)}**")
    st.markdown("---")

    conn = get_db()
    upload_count  = conn.execute("SELECT COUNT(*) FROM uploads").fetchone()[0]
    gen_count     = conn.execute("SELECT COUNT(*) FROM generations").fetchone()[0]
    period_uploads = conn.execute(
        "SELECT id, filename FROM uploads WHERE period = ? ORDER BY upload_time DESC",
        (selected_period,),
    ).fetchall()
    periods_with_data = conn.execute(
        "SELECT DISTINCT period FROM uploads WHERE period != '' ORDER BY period DESC LIMIT 8"
    ).fetchall()
    conn.close()

    st.markdown("### 📁 本期文件")
    if period_uploads:
        for u in period_uploads:
            st.caption(f"📄 {u[1]}")
    else:
        st.caption("暂无文件，请上传")

    st.markdown("---")
    st.markdown("### 📊 平台统计")
    st.caption(f"已上传文件：**{upload_count}** 个")
    st.caption(f"已生成报表：**{gen_count}** 份")

    if periods_with_data:
        st.markdown("---")
        st.markdown("### 🗂️ 已有数据月份")
        for p in periods_with_data:
            st.caption(f"• {period_label(p[0])}")

    st.markdown("---")
    st.caption(f"v0.2 Demo  ·  {datetime.now().strftime('%Y-%m-%d')}")


# ====================================================================
# 顶部标题栏
# ====================================================================
st.markdown(f"""
<div class="platform-header">
    <div>
        <h1>📊 财务报表智能生成平台</h1>
        <div class="sub">中色华鑫马本德矿业有限公司 · 月度财务报表自动化系统</div>
    </div>
    <div class="right">
        当前期间：{period_label(selected_period)}<br>
        <span style="opacity:0.7">演示版 v0.2</span>
    </div>
</div>
""", unsafe_allow_html=True)


# ====================================================================
# 主导航 Tabs
# ====================================================================
tab_home, tab_upload, tab_preview, tab_run, tab_history = st.tabs([
    "🏠  总览",
    "📤  数据上传",
    "👁️  在线查看",
    "🚀  一键运算",
    "📋  历史记录",
])


# ====================================================================
# TAB 1：总览
# ====================================================================
with tab_home:
    # 指标卡片
    c1, c2, c3, c4 = st.columns(4)
    cards = [
        (str(len(period_uploads)), "本期已上传文件"),
        (str(gen_count),           "累计生成报表"),
        ("68 + 16",                "可生成报表数"),
        ("就绪 ✅",                 "AI引擎状态"),
    ]
    for col, (val, lbl) in zip([c1, c2, c3, c4], cards):
        col.markdown(f"""
        <div class="metric-card">
            <div class="val">{val}</div>
            <div class="lbl">{lbl}</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    # 操作流程
    st.markdown("#### 操作流程")
    steps = [
        ("1", "上传数据",  "从NC系统导出<br>Excel文件"),
        ("2", "在线查看",  "验证数据<br>是否正确"),
        ("3", "一键运算",  "自动计算<br>全套报表"),
        ("4", "审核确认",  "在线预览<br>核对数据"),
        ("5", "导出下载",  "Excel / Word<br>报表成品"),
    ]
    cols = st.columns(5)
    for col, (num, title, desc) in zip(cols, steps):
        col.markdown(f"""
        <div class="step-box">
            <div class="step-num">{num}</div>
            <div class="step-title">{title}</div>
            <div class="step-desc">{desc}</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    # 本期文件状态
    st.markdown(f"#### {period_label(selected_period)} 文件状态")
    if period_uploads:
        conn = get_db()
        pdetail = conn.execute(
            "SELECT filename, file_type, sheet_count, row_count, upload_time FROM uploads "
            "WHERE period = ? ORDER BY upload_time DESC",
            (selected_period,),
        ).fetchall()
        conn.close()
        df_status = pd.DataFrame(pdetail, columns=["文件名", "格式", "Sheet数", "行数", "上传时间"])
        df_status["上传时间"] = df_status["上传时间"].apply(lambda x: x[:16].replace("T", " "))
        st.dataframe(df_status, use_container_width=True, hide_index=True)
    else:
        st.info(f"**{period_label(selected_period)}** 暂无已上传文件。请点击「📤 数据上传」选项卡上传数据。")

    st.markdown("---")

    # 可生成的报表类型
    st.markdown("#### 可自动生成的报表")
    rl1, rl2 = st.columns(2)
    with rl1:
        st.markdown("""
        <div class="report-list">
            <h4>📑 公司主报表（68张）</h4>
            资产负债表（人民币版 / 美元版）<br>
            利润表（人民币版 / 美元版）<br>
            现金流量表<br>
            所有者权益变动表<br>
            <br>
            <h4>📊 分析底稿（16张）</h4>
            同比 / 环比分析<br>
            预算执行分析<br>
            成本构成分析
        </div>
        """, unsafe_allow_html=True)
    with rl2:
        st.markdown("""
        <div class="report-list">
            <h4>📈 月度快报（7张）</h4>
            生产经营月度快报<br>
            资金情况月报<br>
            重要指标快报<br>
            <br>
            <h4>📝 月度财务分析报告（Word）</h4>
            含5张数据表格<br>
            AI智能文字分析<br>
            多币种自动折算（美元 / 人民币 / 刚果法郎）
        </div>
        """, unsafe_allow_html=True)


# ====================================================================
# TAB 2：数据上传
# ====================================================================
with tab_upload:
    st.markdown(f"#### 上传 {period_label(selected_period)} 数据文件")
    st.caption("支持从NC系统导出的科目余额表、成本表等文件（.xlsx / .xls / .csv），上限 50MB")

    uploaded_file = st.file_uploader(
        "点击选择或将文件拖拽至此处",
        type=["xlsx", "xls", "csv"],
        help="支持 .xlsx / .xls / .csv",
    )

    if uploaded_file is None:
        st.info("⬆️ 请选择文件。保存后可在「👁️ 在线查看」中浏览内容。")
    else:
        try:
            file_bytes = uploaded_file.read()
            file_ext   = uploaded_file.name.rsplit(".", 1)[-1].lower()
            df_dict    = read_excel_file(file_bytes, file_ext)
            total_rows = sum(len(df) for df in df_dict.values())

            m1, m2, m3 = st.columns(3)
            m1.metric("文件名", uploaded_file.name[:28])
            m2.metric("Sheet 数量", f"{len(df_dict)} 个")
            m3.metric("总数据行", f"{total_rows:,}")

            # Sheet 概览
            st.markdown("**Sheet 列表**")
            sheet_info = [
                {"Sheet名称": name, "行数": len(df), "列数": len(df.columns)}
                for name, df in df_dict.items()
            ]
            st.dataframe(
                pd.DataFrame(sheet_info),
                use_container_width=True,
                hide_index=True,
                height=min(220, 80 + 36 * len(sheet_info)),
            )

            # 数据预览
            st.markdown("**数据预览**")
            sel_sheet = st.selectbox("选择 Sheet", list(df_dict.keys()), key="upload_preview_sheet")
            st.dataframe(
                prepare_for_display(df_dict[sel_sheet].head(30)),
                use_container_width=True,
                height=360,
            )

            st.markdown("---")
            if st.button("💾  确认保存到平台", type="primary", use_container_width=True):
                try:
                    save_path = period_data_dir(selected_period) / uploaded_file.name
                    with open(save_path, "wb") as f:
                        f.write(file_bytes)
                    conn = get_db()
                    cursor = conn.execute(
                        "INSERT INTO uploads (period, filename, file_type, sheet_count, row_count, upload_time, file_path) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (selected_period, uploaded_file.name, file_ext,
                         len(df_dict), total_rows, datetime.now().isoformat(), str(save_path)),
                    )
                    upload_id = cursor.lastrowid
                    conn.commit()
                    conn.close()
                    # 缓存文件内容（< 10MB），用于云端在线查看
                    if len(file_bytes) < 10 * 1024 * 1024:
                        st.session_state[f"file_cache_{upload_id}"] = file_bytes
                    st.success(
                        f"✅ 已保存：【{period_label(selected_period)}】{uploaded_file.name}"
                        f"（{len(df_dict)} sheets，{total_rows:,} 行）"
                    )
                    st.info("💡 前往「👁️ 在线查看」选项卡可浏览完整内容。")
                    st.balloons()
                except Exception as e:
                    st.error(f"保存失败：{e}")
        except Exception as e:
            st.error(f"文件解析失败：{e}")


# ====================================================================
# TAB 3：在线查看
# ====================================================================
with tab_preview:
    st.markdown("#### 在线查看已上传文件")

    show_all = st.checkbox("显示全部月份文件", value=False)

    conn = get_db()
    if show_all:
        preview_files = conn.execute(
            "SELECT id, period, filename, file_path, sheet_count, row_count FROM uploads "
            "ORDER BY period DESC, upload_time DESC"
        ).fetchall()
    else:
        preview_files = conn.execute(
            "SELECT id, period, filename, file_path, sheet_count, row_count FROM uploads "
            "WHERE period = ? ORDER BY upload_time DESC",
            (selected_period,),
        ).fetchall()
    conn.close()

    if not preview_files:
        msg = "平台暂无已上传文件，请先上传。" if show_all else (
            f"**{period_label(selected_period)}** 暂无文件。"
            "可勾选「显示全部月份文件」查看其他月份，或先上传文件。"
        )
        st.info(msg)
    else:
        selected_pf = st.selectbox(
            "选择文件",
            options=preview_files,
            format_func=lambda x: (
                f"【{period_label(x[1]) if x[1] else '未分类'}】{x[2]}"
                f"（{x[4]} 个sheet，{x[5]:,} 行）"
            ),
            key="preview_file_select",
        )

        if selected_pf:
            fpath, fname = selected_pf[3], selected_pf[2]
            cache_key    = f"file_cache_{selected_pf[0]}"
            df_dict      = None
            source_tag   = ""

            if fpath and os.path.exists(fpath):
                try:
                    ext    = fpath.rsplit(".", 1)[-1].lower()
                    df_dict    = read_excel_file(fpath, ext)
                    source_tag = "服务器磁盘"
                except Exception as e:
                    st.error(f"读取文件失败：{e}")
            elif cache_key in st.session_state:
                try:
                    ext        = fname.rsplit(".", 1)[-1].lower()
                    df_dict    = read_excel_file(st.session_state[cache_key], ext)
                    source_tag = "会话缓存"
                except Exception as e:
                    st.error(f"读取缓存失败：{e}")
            else:
                st.warning(
                    "⚠️ 文件不在服务器上（云端部署的会话缓存已过期），请重新上传该文件。"
                )

            if df_dict:
                st.success(
                    f"✅ 已加载 **{fname}**（{source_tag}，共 {len(df_dict)} 个 Sheet）"
                )
                sheet_names = list(df_dict.keys())
                if len(sheet_names) == 1:
                    sn = sheet_names[0]
                    df = df_dict[sn]
                    st.caption(f"Sheet: {sn} — {len(df):,} 行 × {len(df.columns)} 列")
                    st.dataframe(prepare_for_display(df), use_container_width=True, height=540)
                else:
                    ptabs = st.tabs([f"📄 {sn}" for sn in sheet_names])
                    for ptab, sn in zip(ptabs, sheet_names):
                        with ptab:
                            df = df_dict[sn]
                            st.caption(f"{len(df):,} 行 × {len(df.columns)} 列")
                            st.dataframe(
                                prepare_for_display(df),
                                use_container_width=True,
                                height=500,
                            )


# ====================================================================
# TAB 4：一键运算
# ====================================================================
with tab_run:
    st.markdown(f"#### 一键运算 — {period_label(selected_period)}")
    st.caption("选择本期数据文件，系统将自动计算并生成全套财务报表。")

    conn = get_db()
    uploads_cur = conn.execute(
        "SELECT id, filename, upload_time, period FROM uploads WHERE period = ? ORDER BY upload_time DESC",
        (selected_period,),
    ).fetchall()
    uploads_all = conn.execute(
        "SELECT id, filename, upload_time, period FROM uploads ORDER BY upload_time DESC"
    ).fetchall()
    conn.close()

    display_uploads = uploads_cur or uploads_all

    if not display_uploads:
        st.warning("请先上传数据文件。")
    else:
        if not uploads_cur:
            st.warning(f"**{period_label(selected_period)}** 暂无文件，显示全部月份文件。")

        col1, col2 = st.columns(2)
        with col1:
            selected_upload = st.selectbox(
                "① 选择数据源文件",
                options=display_uploads,
                format_func=lambda x: f"【{period_label(x[3]) if x[3] else '未分类'}】{x[1]}",
            )
        with col2:
            ai_model = st.selectbox("② 选择处理引擎", [
                "本地规则引擎（离线·推荐）",
                "DeepSeek-V3（AI增强）",
                "Claude Sonnet 4（高精度）",
                "Gemini 2.5 Pro（多语言）",
            ])

        # 报表选择
        st.markdown("**③ 选择要生成的报表**")
        rc1, rc2, rc3, rc4 = st.columns(4)
        gen_main  = rc1.checkbox("公司主报表（68张）", value=True)
        gen_draft = rc2.checkbox("分析底稿（16张）",   value=True)
        gen_flash = rc3.checkbox("月度快报（7张）",    value=False)
        gen_word  = rc4.checkbox("Word分析报告",       value=False)

        output_fmt = st.selectbox("④ 输出格式", [
            "Excel (.xlsx)",
            "Excel + Word 全套",
        ])

        st.markdown("---")
        if st.button("🚀  开始一键运算", type="primary", use_container_width=True):
            prog  = st.progress(0)
            status = st.empty()
            for pct, msg in [
                (10,  "📂 读取源数据文件..."),
                (25,  "🔍 解析科目编码与数据结构..."),
                (40,  "🔗 加载映射规则..."),
                (55,  "🤖 引擎处理数据..."),
                (70,  "📊 执行数据映射与计算..."),
                (85,  "📝 生成报表文件..."),
                (95,  "✅ 数据校验与质量检查..."),
                (100, "🎉 运算完成！"),
            ]:
                prog.progress(pct)
                status.markdown(f"**{msg}**")
                time.sleep(0.55)

            conn = get_db()
            file_info = conn.execute(
                "SELECT file_path, filename FROM uploads WHERE id = ?",
                (selected_upload[0],),
            ).fetchone()
            conn.close()

            if file_info and os.path.exists(file_info[0]):
                try:
                    ext     = file_info[0].rsplit(".", 1)[-1].lower()
                    src_xls = pd.ExcelFile(
                        file_info[0], engine="xlrd" if ext == "xls" else "openpyxl"
                    )
                    first_df = src_xls.parse(src_xls.sheet_names[0])

                    out_dir     = period_output_dir(selected_period)
                    output_name = f"报表_{period_label(selected_period)}_{datetime.now().strftime('%H%M%S')}.xlsx"
                    output_path = out_dir / output_name

                    with pd.ExcelWriter(str(output_path), engine="openpyxl") as writer:
                        first_df.to_excel(writer, sheet_name="原始数据", index=False)
                        numeric_cols = first_df.select_dtypes(include="number").columns.tolist()
                        if numeric_cols:
                            summary = [
                                {"指标": c, "合计": first_df[c].sum(),
                                 "平均": first_df[c].mean(),
                                 "最大": first_df[c].max(),
                                 "最小": first_df[c].min()}
                                for c in numeric_cols
                            ]
                            pd.DataFrame(summary).to_excel(writer, sheet_name="数据汇总", index=False)

                    conn = get_db()
                    conn.execute(
                        "INSERT INTO generations (period, source_upload_id, ai_model, output_filename, "
                        "output_path, status, created_at, duration_seconds) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (selected_period, selected_upload[0], ai_model, output_name,
                         str(output_path), "已完成", datetime.now().isoformat(), 4.8),
                    )
                    conn.commit()
                    conn.close()

                    st.markdown("---")
                    st.markdown("#### 运算结果")
                    r1, r2, r3, r4 = st.columns(4)
                    r1.metric("输出文件", output_name[:22])
                    r2.metric("数据行数", f"{len(first_df):,}")
                    r3.metric("Sheet 数", len(src_xls.sheet_names))
                    r4.metric("处理耗时", "4.8 秒")

                    with open(output_path, "rb") as f:
                        st.download_button(
                            "📥 下载报表文件",
                            data=f.read(),
                            file_name=output_name,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True,
                            type="primary",
                        )
                    st.balloons()
                except Exception as e:
                    st.error(f"运算过程出错：{e}")
            else:
                st.success("✅ 运算完成（演示模式）")
                st.info("请先上传实际数据文件以获取真实输出。")


# ====================================================================
# TAB 5：历史记录
# ====================================================================
with tab_history:
    st.markdown("#### 历史记录")

    hist_filter_opts = ["全部月份"] + period_options()
    hist_period = st.selectbox(
        "按月份筛选",
        options=hist_filter_opts,
        format_func=lambda x: "全部月份" if x == "全部月份" else period_label(x),
        index=0,
        key="hist_period",
    )

    htab1, htab2, htab3 = st.tabs(["📤 上传记录", "📊 生成记录", "💿 存储统计"])

    # ── 上传记录 ──
    with htab1:
        conn = get_db()
        if hist_period == "全部月份":
            up_data = conn.execute(
                "SELECT id, period, filename, file_type, sheet_count, row_count, upload_time, status "
                "FROM uploads ORDER BY period DESC, upload_time DESC"
            ).fetchall()
        else:
            up_data = conn.execute(
                "SELECT id, period, filename, file_type, sheet_count, row_count, upload_time, status "
                "FROM uploads WHERE period = ? ORDER BY upload_time DESC",
                (hist_period,),
            ).fetchall()
        conn.close()

        if not up_data:
            st.info("暂无上传记录。")
        else:
            df_up = pd.DataFrame(
                up_data, columns=["ID", "月份", "文件名", "格式", "Sheet数", "总行数", "上传时间", "状态"]
            )
            df_up["月份"]    = df_up["月份"].apply(lambda x: period_label(x) if x else "未分类")
            df_up["上传时间"] = df_up["上传时间"].apply(lambda x: x[:16].replace("T", " "))
            st.dataframe(df_up, use_container_width=True, hide_index=True)

            del_id = st.number_input("输入 ID 删除记录", min_value=0, step=1, value=0, key="del_id")
            if del_id > 0 and st.button("🗑️ 删除该记录", type="secondary"):
                conn = get_db()
                row = conn.execute("SELECT file_path FROM uploads WHERE id = ?", (del_id,)).fetchone()
                if row and row[0] and os.path.exists(row[0]):
                    os.remove(row[0])
                conn.execute("DELETE FROM uploads WHERE id = ?", (del_id,))
                conn.commit()
                conn.close()
                st.success(f"已删除记录 #{del_id}")
                st.rerun()

    # ── 生成记录 ──
    with htab2:
        conn = get_db()
        if hist_period == "全部月份":
            gen_data = conn.execute("""
                SELECT g.id, g.period, u.filename, g.ai_model, g.output_filename,
                       g.status, g.created_at, g.duration_seconds
                FROM generations g
                LEFT JOIN uploads u ON g.source_upload_id = u.id
                ORDER BY g.period DESC, g.created_at DESC
            """).fetchall()
        else:
            gen_data = conn.execute("""
                SELECT g.id, g.period, u.filename, g.ai_model, g.output_filename,
                       g.status, g.created_at, g.duration_seconds
                FROM generations g
                LEFT JOIN uploads u ON g.source_upload_id = u.id
                WHERE g.period = ?
                ORDER BY g.created_at DESC
            """, (hist_period,)).fetchall()
        conn.close()

        if not gen_data:
            st.info("暂无生成记录。")
        else:
            df_gen = pd.DataFrame(
                gen_data, columns=["ID", "月份", "源文件", "引擎", "输出文件", "状态", "生成时间", "耗时(秒)"]
            )
            df_gen["月份"]    = df_gen["月份"].apply(lambda x: period_label(x) if x else "未分类")
            df_gen["生成时间"] = df_gen["生成时间"].apply(lambda x: x[:16].replace("T", " "))
            st.dataframe(df_gen, use_container_width=True, hide_index=True)

            conn = get_db()
            outputs = conn.execute(
                "SELECT output_filename, output_path FROM generations WHERE status = '已完成'"
            ).fetchall()
            conn.close()
            for out in outputs:
                if out[1] and os.path.exists(out[1]):
                    with open(out[1], "rb") as f:
                        st.download_button(
                            f"📥 {out[0]}",
                            data=f.read(),
                            file_name=out[0],
                            key=f"dl_{out[0]}",
                        )

    # ── 存储统计 ──
    with htab3:
        def get_dir_size(path):
            return sum(p.stat().st_size for p in Path(path).rglob("*") if p.is_file())

        data_sz   = get_dir_size(DATA_DIR)
        output_sz = get_dir_size(OUTPUT_DIR)
        map_sz    = get_dir_size(MAPPING_DIR)

        s1, s2, s3, s4 = st.columns(4)
        s1.metric("数据文件",  fmt_size(data_sz))
        s2.metric("生成报表",  fmt_size(output_sz))
        s3.metric("映射规则",  fmt_size(map_sz))
        s4.metric("总计",      fmt_size(data_sz + output_sz + map_sz))

        st.markdown("---")
        st.markdown("**数据库**")
        db_sz = os.path.getsize(str(DB_PATH)) if DB_PATH.exists() else 0
        st.caption(f"文件：`{DB_PATH.name}`　大小：{fmt_size(db_sz)}")
        conn = get_db()
        for t in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall():
            cnt = conn.execute(f"SELECT COUNT(*) FROM {t[0]}").fetchone()[0]
            st.caption(f"表 `{t[0]}`：{cnt} 条记录")
        conn.close()
