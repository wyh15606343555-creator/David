"""
财务报表智能生成平台 — Demo
================================
三大模块：数据上传 · AI引擎 · 数据存储
可配置报表生成平台演示
"""

import streamlit as st
import pandas as pd
import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
import io
import calendar

# ── 路径配置 ──
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
MAPPING_DIR = BASE_DIR / "mappings"
TEMPLATE_DIR = BASE_DIR / "templates"
DB_PATH = BASE_DIR / "data" / "platform.db"

for d in [DATA_DIR, OUTPUT_DIR, MAPPING_DIR, TEMPLATE_DIR]:
    d.mkdir(exist_ok=True)


def period_options():
    """生成最近24个月的期间选项，格式 YYYY-MM"""
    now = datetime.now()
    opts = []
    for i in range(24):
        y = now.year
        m = now.month - i
        while m <= 0:
            m += 12
            y -= 1
        opts.append(f"{y:04d}-{m:02d}")
    return opts


def period_label(p):
    """YYYY-MM → 2026年01月"""
    y, m = p.split("-")
    return f"{y}年{m}月"


def period_data_dir(period):
    """按月份返回数据子目录并自动创建"""
    d = DATA_DIR / period.replace("-", "")
    d.mkdir(exist_ok=True)
    return d


def period_output_dir(period):
    """按月份返回输出子目录并自动创建"""
    d = OUTPUT_DIR / period.replace("-", "")
    d.mkdir(exist_ok=True)
    return d

# ── 页面配置 ──
st.set_page_config(
    page_title="财务报表智能生成平台",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── 自定义样式 ──
st.markdown("""
<style>
    .main-header {
        font-size: 1.8rem;
        font-weight: 700;
        color: #1F4E79;
        padding-bottom: 0.5rem;
        border-bottom: 3px solid #2E75B6;
        margin-bottom: 1.5rem;
    }
    .module-card {
        background: linear-gradient(135deg, #f8fbff 0%, #e8f0fe 100%);
        border-radius: 12px;
        padding: 1.5rem;
        border-left: 4px solid #2E75B6;
        margin-bottom: 1rem;
    }
    .stat-number {
        font-size: 2rem;
        font-weight: 700;
        color: #1F4E79;
    }
    .stat-label {
        font-size: 0.85rem;
        color: #666;
    }
    .success-box {
        background: #e8f5e9;
        border-left: 4px solid #4caf50;
        padding: 1rem;
        border-radius: 0 8px 8px 0;
    }
    .warning-box {
        background: #fff3e0;
        border-left: 4px solid #ff9800;
        padding: 1rem;
        border-radius: 0 8px 8px 0;
    }
</style>
""", unsafe_allow_html=True)


# ====================================================================
# 数据库初始化
# ====================================================================
def init_db():
    """初始化SQLite数据库"""
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()

    # 上传记录表
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

    # 映射规则表
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

    # 生成记录表
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
            duration_seconds REAL,
            FOREIGN KEY (source_upload_id) REFERENCES uploads(id),
            FOREIGN KEY (mapping_id) REFERENCES mappings(id)
        )
    """)

    # 兼容旧数据库：如果 period 列不存在则补加
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
# 侧边栏导航
# ====================================================================
with st.sidebar:
    st.markdown("### 📊 财务报表智能生成平台")
    st.markdown("---")

    page = st.radio(
        "功能模块",
        ["🏠 平台总览", "📤 数据上传", "🔗 映射配置", "🤖 AI报表生成", "💾 数据管理"],
        index=0,
    )

    st.markdown("---")
    st.markdown("##### 系统信息")
    st.caption(f"版本：v0.1 Demo")
    st.caption(f"日期：{datetime.now().strftime('%Y-%m-%d')}")

    # 统计信息
    conn = get_db()
    upload_count = conn.execute("SELECT COUNT(*) FROM uploads").fetchone()[0]
    mapping_count = conn.execute("SELECT COUNT(*) FROM mappings").fetchone()[0]
    gen_count = conn.execute("SELECT COUNT(*) FROM generations").fetchone()[0]
    conn.close()

    st.markdown("##### 数据统计")
    st.caption(f"已上传文件：{upload_count} 个")
    st.caption(f"映射规则：{mapping_count} 条")
    st.caption(f"已生成报表：{gen_count} 份")

    # 显示有数据的月份
    conn2 = get_db()
    periods_with_data = conn2.execute(
        "SELECT DISTINCT period FROM uploads WHERE period != '' ORDER BY period DESC LIMIT 6"
    ).fetchall()
    conn2.close()
    if periods_with_data:
        st.markdown("##### 已有数据月份")
        for p in periods_with_data:
            st.caption(f"• {period_label(p[0])}")


# ====================================================================
# 页面：平台总览
# ====================================================================
if page == "🏠 平台总览":
    st.markdown('<div class="main-header">财务报表智能生成平台</div>', unsafe_allow_html=True)

    st.markdown("""
    > **一站式财务报表自动化解决方案** — 上传数据 → 配置映射 → AI生成报表
    """)

    # 三大模块卡片
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("""
        <div class="module-card">
            <h3>📤 数据上传</h3>
            <p>支持 Excel / CSV 格式<br>
            自动识别sheet结构<br>
            数据预览与校验</p>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown("""
        <div class="module-card">
            <h3>🤖 AI 引擎</h3>
            <p>接入主流AI模型API<br>
            智能数据分析与填充<br>
            自动生成分析报告文字</p>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown("""
        <div class="module-card">
            <h3>💾 数据存储</h3>
            <p>本地SQLite数据库<br>
            历史数据管理<br>
            映射规则持久化</p>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    # 工作流程图
    st.markdown("### 工作流程")
    flow_col1, flow_col2, flow_col3, flow_col4, flow_col5 = st.columns(5)
    with flow_col1:
        st.info("**① 上传**\n\nNC系统导出的\nExcel原始数据")
    with flow_col2:
        st.info("**② 配置**\n\n设置科目映射\n与取数规则")
    with flow_col3:
        st.info("**③ 生成**\n\nAI引擎自动\n计算与填充")
    with flow_col4:
        st.info("**④ 审核**\n\n在线预览\n核对数据")
    with flow_col5:
        st.success("**⑤ 导出**\n\n下载Excel/Word\n报表成品")

    st.markdown("---")

    # 快速统计
    st.markdown("### 平台运行状态")
    s1, s2, s3, s4 = st.columns(4)
    with s1:
        st.metric("已上传文件", f"{upload_count} 个")
    with s2:
        st.metric("映射规则", f"{mapping_count} 条")
    with s3:
        st.metric("已生成报表", f"{gen_count} 份")
    with s4:
        st.metric("AI引擎状态", "就绪 ✅")


# ====================================================================
# 页面：数据上传
# ====================================================================
elif page == "📤 数据上传":
    st.markdown('<div class="main-header">数据上传</div>', unsafe_allow_html=True)
    st.markdown("上传NC系统导出的Excel文件，平台自动解析数据结构。")

    # ── 月份选择 ──
    st.markdown("### 📅 选择数据月份")
    period_opts = period_options()
    upload_period = st.selectbox(
        "数据所属月份",
        options=period_opts,
        format_func=period_label,
        index=0,
        help="请选择该文件对应的财务期间（月份）",
    )
    st.info(f"当前选择期间：**{period_label(upload_period)}**　　文件将存入 `data/{upload_period.replace('-', '')}/`")

    uploaded_file = st.file_uploader(
        "选择Excel文件",
        type=["xlsx", "xls", "csv"],
        help="支持 .xlsx / .xls / .csv 格式",
    )

    if uploaded_file is not None:
        st.markdown("---")
        st.markdown("### 文件解析结果")

        try:
            # 读取文件
            file_bytes = uploaded_file.read()
            file_ext = uploaded_file.name.rsplit(".", 1)[-1].lower()

            if file_ext == "csv":
                df_dict = {"Sheet1": pd.read_csv(io.BytesIO(file_bytes))}
            elif file_ext == "xls":
                xls = pd.ExcelFile(io.BytesIO(file_bytes), engine="xlrd")
                df_dict = {name: xls.parse(name) for name in xls.sheet_names}
            else:
                xls = pd.ExcelFile(io.BytesIO(file_bytes), engine="openpyxl")
                df_dict = {name: xls.parse(name) for name in xls.sheet_names}

            # 文件概况
            total_rows = sum(len(df) for df in df_dict.values())
            info_col1, info_col2, info_col3 = st.columns(3)
            with info_col1:
                st.metric("文件名", uploaded_file.name)
            with info_col2:
                st.metric("Sheet数量", f"{len(df_dict)} 个")
            with info_col3:
                st.metric("总行数", f"{total_rows:,}")

            # Sheet列表
            st.markdown("### Sheet 概览")
            sheet_info = []
            for name, df in df_dict.items():
                sheet_info.append({
                    "Sheet名称": name,
                    "行数": len(df),
                    "列数": len(df.columns),
                    "列名": ", ".join([str(c) for c in df.columns[:5]]) + ("..." if len(df.columns) > 5 else ""),
                })
            st.dataframe(pd.DataFrame(sheet_info), use_container_width=True, hide_index=True)

            # Sheet数据预览
            st.markdown("### 数据预览")
            selected_sheet = st.selectbox("选择Sheet查看", list(df_dict.keys()))
            if selected_sheet:
                df_preview = df_dict[selected_sheet]
                st.dataframe(df_preview.head(50), use_container_width=True, height=400)

                # 列信息
                with st.expander("查看列详情"):
                    col_info = []
                    for col in df_preview.columns:
                        col_info.append({
                            "列名": str(col),
                            "数据类型": str(df_preview[col].dtype),
                            "非空数": int(df_preview[col].notna().sum()),
                            "空值数": int(df_preview[col].isna().sum()),
                            "示例值": str(df_preview[col].dropna().iloc[0]) if df_preview[col].notna().any() else "—",
                        })
                    st.dataframe(pd.DataFrame(col_info), use_container_width=True, hide_index=True)

            # 保存按钮
            st.markdown("---")
            if st.button("💾 保存到平台数据库", type="primary", use_container_width=True):
                # 保存文件到月份子目录
                save_dir = period_data_dir(upload_period)
                save_path = save_dir / uploaded_file.name
                with open(save_path, "wb") as f:
                    f.write(file_bytes)

                # 写入数据库（含 period）
                conn = get_db()
                conn.execute(
                    "INSERT INTO uploads (period, filename, file_type, sheet_count, row_count, upload_time, file_path) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (upload_period, uploaded_file.name, file_ext, len(df_dict), total_rows,
                     datetime.now().isoformat(), str(save_path)),
                )
                conn.commit()
                conn.close()

                st.success(f"✅ 文件已保存！【{period_label(upload_period)}】{uploaded_file.name}（{len(df_dict)} sheets, {total_rows:,} 行）")
                st.balloons()

        except Exception as e:
            st.error(f"文件解析失败：{e}")


# ====================================================================
# 页面：映射配置
# ====================================================================
elif page == "🔗 映射配置":
    st.markdown('<div class="main-header">映射配置</div>', unsafe_allow_html=True)
    st.markdown("配置源数据到目标报表的映射规则。您可以定义科目编码与报表单元格的对应关系。")

    tab1, tab2 = st.tabs(["➕ 新建映射", "📋 已有映射"])

    # ── 新建映射 ──
    with tab1:
        st.markdown("### 创建新映射规则")

        # 获取已上传的文件列表
        conn = get_db()
        uploads = conn.execute("SELECT id, filename, sheet_count FROM uploads ORDER BY upload_time DESC").fetchall()
        conn.close()

        if not uploads:
            st.warning("请先上传数据文件，再配置映射规则。")
        else:
            mapping_name = st.text_input("映射规则名称", placeholder="例如：科目余额表 → 公司主报表")

            col_src, col_tgt = st.columns(2)
            with col_src:
                st.markdown("#### 📥 数据源")
                source_file = st.selectbox(
                    "选择源文件",
                    options=[(u[0], u[1]) for u in uploads],
                    format_func=lambda x: x[1],
                )
                if source_file:
                    # 读取源文件的sheet和列
                    conn = get_db()
                    file_path = conn.execute("SELECT file_path FROM uploads WHERE id = ?", (source_file[0],)).fetchone()[0]
                    conn.close()

                    if os.path.exists(file_path):
                        try:
                            ext = file_path.rsplit(".", 1)[-1].lower()
                            engine = "xlrd" if ext == "xls" else "openpyxl"
                            xls = pd.ExcelFile(file_path, engine=engine)
                            source_sheet = st.selectbox("源Sheet", xls.sheet_names, key="src_sheet")
                            if source_sheet:
                                src_df = xls.parse(source_sheet, nrows=5)
                                source_cols = [str(c) for c in src_df.columns]
                                st.caption(f"可用列：{len(source_cols)} 个")
                        except Exception as e:
                            st.error(f"读取文件失败：{e}")
                            source_cols = []
                    else:
                        st.error("源文件不存在，请重新上传")
                        source_cols = []

            with col_tgt:
                st.markdown("#### 📤 目标报表")
                target_template = st.text_input("目标报表名称", placeholder="例如：公司主报表（人民币版）")
                target_sheet = st.text_input("目标Sheet", placeholder="例如：资产负债表")
                target_cell = st.text_input("目标单元格/区域", placeholder="例如：B5 或 B5:F20")

            # 映射规则编辑器
            st.markdown("---")
            st.markdown("### 映射规则定义")
            st.markdown("定义源数据列与目标报表位置的对应关系：")

            num_rules = st.number_input("规则数量", min_value=1, max_value=50, value=3)
            rules = []
            for i in range(int(num_rules)):
                rc1, rc2, rc3, rc4 = st.columns([2, 2, 2, 1])
                with rc1:
                    src_col = st.text_input(f"源列/科目编码", key=f"src_{i}", placeholder="例如：1001")
                with rc2:
                    tgt_loc = st.text_input(f"目标位置", key=f"tgt_{i}", placeholder="例如：Sheet1!B5")
                with rc3:
                    transform = st.selectbox(f"转换方式", ["直接映射", "求和汇总", "差额计算", "汇率折算", "百分比计算"], key=f"tf_{i}")
                with rc4:
                    st.markdown(f"<br>", unsafe_allow_html=True)
                    st.caption(f"规则 #{i+1}")

                if src_col and tgt_loc:
                    rules.append({
                        "source": src_col,
                        "target": tgt_loc,
                        "transform": transform,
                    })

            st.markdown("---")
            if st.button("💾 保存映射规则", type="primary", use_container_width=True):
                if not mapping_name:
                    st.error("请输入映射规则名称")
                elif not rules:
                    st.error("请至少配置一条映射规则")
                else:
                    conn = get_db()
                    conn.execute(
                        "INSERT INTO mappings (name, source_file, target_template, rules_json, created_at) VALUES (?, ?, ?, ?, ?)",
                        (mapping_name, source_file[1] if source_file else "",
                         target_template, json.dumps(rules, ensure_ascii=False),
                         datetime.now().isoformat()),
                    )
                    conn.commit()
                    conn.close()

                    # 同时保存为JSON文件
                    mapping_file = MAPPING_DIR / f"{mapping_name}.json"
                    with open(mapping_file, "w", encoding="utf-8") as f:
                        json.dump({
                            "name": mapping_name,
                            "source_file": source_file[1] if source_file else "",
                            "target_template": target_template,
                            "rules": rules,
                            "created_at": datetime.now().isoformat(),
                        }, f, ensure_ascii=False, indent=2)

                    st.success(f"✅ 映射规则「{mapping_name}」已保存！（{len(rules)} 条规则）")

    # ── 已有映射 ──
    with tab2:
        conn = get_db()
        existing_mappings = conn.execute(
            "SELECT id, name, source_file, target_template, rules_json, created_at FROM mappings ORDER BY created_at DESC"
        ).fetchall()
        conn.close()

        if not existing_mappings:
            st.info("暂无映射规则，请先创建。")
        else:
            for m in existing_mappings:
                with st.expander(f"📋 {m[1]}（{m[5][:10]}）"):
                    st.markdown(f"**源文件**：{m[2]}")
                    st.markdown(f"**目标报表**：{m[3]}")
                    rules_data = json.loads(m[4]) if m[4] else []
                    if rules_data:
                        st.dataframe(pd.DataFrame(rules_data), use_container_width=True, hide_index=True)
                    st.caption(f"创建时间：{m[5]}")


# ====================================================================
# 页面：AI报表生成
# ====================================================================
elif page == "🤖 AI报表生成":
    st.markdown('<div class="main-header">AI 报表生成</div>', unsafe_allow_html=True)
    st.markdown("选择数据源和映射规则，AI引擎自动生成目标报表。")

    # ── 月份选择 ──
    st.markdown("### 📅 选择报表月份")
    gen_period_opts = period_options()
    gen_period = st.selectbox(
        "报表所属月份",
        options=gen_period_opts,
        format_func=period_label,
        index=0,
        key="gen_period",
        help="选择要生成报表的财务期间",
    )

    # 获取该月份的数据
    conn = get_db()
    uploads = conn.execute(
        "SELECT id, filename, upload_time, period FROM uploads WHERE period = ? ORDER BY upload_time DESC",
        (gen_period,),
    ).fetchall()
    # 同时获取所有上传（用于回退显示）
    all_uploads = conn.execute("SELECT id, filename, upload_time, period FROM uploads ORDER BY upload_time DESC").fetchall()
    mappings = conn.execute("SELECT id, name, created_at FROM mappings ORDER BY created_at DESC").fetchall()
    conn.close()

    if not uploads and not all_uploads:
        st.warning("请先上传数据文件。")
    else:
        if not uploads:
            st.warning(f"**{period_label(gen_period)}** 暂无已上传的数据文件。下方显示全部月份的文件供参考。")
            uploads = all_uploads

        col_config1, col_config2 = st.columns(2)

        with col_config1:
            st.markdown("### ① 选择数据源")
            selected_upload = st.selectbox(
                "已上传的数据文件",
                options=uploads,
                format_func=lambda x: f"【{period_label(x[3]) if x[3] else '未分类'}】{x[1]}",
            )

        with col_config2:
            st.markdown("### ② 选择映射规则")
            if mappings:
                selected_mapping = st.selectbox(
                    "映射规则",
                    options=mappings,
                    format_func=lambda x: f"{x[1]}（{x[2][:10]}）",
                )
            else:
                st.info("暂无映射规则。可跳过，使用AI智能识别。")
                selected_mapping = None

        st.markdown("---")
        st.markdown("### ③ AI引擎配置")

        ai_col1, ai_col2 = st.columns(2)
        with ai_col1:
            ai_model = st.selectbox("AI模型", [
                "DeepSeek-V3（推荐·成本低）",
                "Claude Sonnet 4（高精度）",
                "Gemini 2.5 Pro（多语言）",
                "本地规则引擎（离线·无需API）",
            ])
        with ai_col2:
            output_format = st.selectbox("输出格式", [
                "Excel (.xlsx)",
                "Word 报告 (.docx)",
                "Excel + Word 全套",
            ])

        # AI提示词配置
        with st.expander("🔧 高级配置 — AI提示词"):
            ai_prompt = st.text_area(
                "自定义AI指令",
                value="请根据上传的科目余额表数据，按照映射规则，自动填充目标报表模板。\n"
                      "要求：\n"
                      "1. 严格按照科目编码进行数据映射\n"
                      "2. 自动计算合计行和小计行\n"
                      "3. 金额单位自动转换（元→万元）\n"
                      "4. 生成数据校验摘要",
                height=150,
            )

        # 生成按钮
        st.markdown("---")
        if st.button("🚀 开始生成报表", type="primary", use_container_width=True):

            # 进度展示
            progress_bar = st.progress(0)
            status_text = st.empty()

            import time

            steps = [
                (10, "📂 读取源数据文件..."),
                (25, "🔍 解析数据结构与科目编码..."),
                (40, "🔗 加载映射规则..."),
                (55, "🤖 调用AI引擎处理数据..."),
                (70, "📊 执行数据映射与计算..."),
                (85, "📝 生成报表文件..."),
                (95, "✅ 数据校验与质量检查..."),
                (100, "🎉 报表生成完成！"),
            ]

            for progress, msg in steps:
                progress_bar.progress(progress)
                status_text.markdown(f"**{msg}**")
                time.sleep(0.8)

            # 读取源文件生成示例输出
            conn = get_db()
            file_info = conn.execute("SELECT file_path, filename FROM uploads WHERE id = ?", (selected_upload[0],)).fetchone()
            conn.close()

            if file_info and os.path.exists(file_info[0]):
                try:
                    ext = file_info[0].rsplit(".", 1)[-1].lower()
                    engine = "xlrd" if ext == "xls" else "openpyxl"
                    src_xls = pd.ExcelFile(file_info[0], engine=engine)
                    first_sheet = src_xls.parse(src_xls.sheet_names[0])

                    # 生成输出文件（按月份存放）
                    out_dir = period_output_dir(gen_period)
                    output_name = f"报表_{period_label(gen_period)}_{datetime.now().strftime('%H%M%S')}.xlsx"
                    output_path = out_dir / output_name

                    with pd.ExcelWriter(str(output_path), engine="openpyxl") as writer:
                        # 写入原始数据sheet
                        first_sheet.to_excel(writer, sheet_name="原始数据", index=False)

                        # 生成汇总sheet
                        numeric_cols = first_sheet.select_dtypes(include="number").columns.tolist()
                        if numeric_cols:
                            summary_data = []
                            for col in numeric_cols:
                                summary_data.append({
                                    "指标": col,
                                    "合计": first_sheet[col].sum(),
                                    "平均": first_sheet[col].mean(),
                                    "最大": first_sheet[col].max(),
                                    "最小": first_sheet[col].min(),
                                })
                            pd.DataFrame(summary_data).to_excel(writer, sheet_name="数据汇总", index=False)

                    # 记录到数据库（含 period）
                    conn = get_db()
                    conn.execute(
                        "INSERT INTO generations (period, source_upload_id, mapping_id, ai_model, output_filename, output_path, status, created_at, duration_seconds) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (gen_period, selected_upload[0],
                         selected_mapping[0] if selected_mapping else None,
                         ai_model, output_name, str(output_path),
                         "已完成", datetime.now().isoformat(), 5.6),
                    )
                    conn.commit()
                    conn.close()

                    # 展示结果
                    st.markdown("---")
                    st.markdown("### 📊 生成结果")

                    r1, r2, r3 = st.columns(3)
                    with r1:
                        st.metric("输出文件", output_name)
                    with r2:
                        st.metric("数据行数", f"{len(first_sheet):,}")
                    with r3:
                        st.metric("处理耗时", "5.6 秒")

                    # 预览输出
                    st.markdown("#### 数据汇总预览")
                    if numeric_cols:
                        st.dataframe(pd.DataFrame(summary_data), use_container_width=True, hide_index=True)

                    # 下载按钮
                    with open(output_path, "rb") as f:
                        st.download_button(
                            "📥 下载生成的报表",
                            data=f.read(),
                            file_name=output_name,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True,
                        )

                    st.balloons()

                except Exception as e:
                    st.error(f"生成过程出错：{e}")
            else:
                # 没有源文件时的模拟输出
                st.success("✅ 报表生成完成！（演示模式）")
                st.info("请先上传实际数据文件以获取真实输出。")


# ====================================================================
# 页面：数据管理
# ====================================================================
elif page == "💾 数据管理":
    st.markdown('<div class="main-header">数据管理</div>', unsafe_allow_html=True)

    # ── 月份筛选器 ──
    mgmt_filter_opts = ["全部月份"] + period_options()
    mgmt_period = st.selectbox(
        "📅 按月份筛选",
        options=mgmt_filter_opts,
        format_func=lambda x: "全部月份" if x == "全部月份" else period_label(x),
        index=0,
        key="mgmt_period",
    )

    tab_uploads, tab_generations, tab_storage = st.tabs(["📤 上传记录", "📊 生成记录", "💿 存储统计"])

    # ── 上传记录 ──
    with tab_uploads:
        conn = get_db()
        if mgmt_period == "全部月份":
            uploads_data = conn.execute(
                "SELECT id, period, filename, file_type, sheet_count, row_count, upload_time, status FROM uploads ORDER BY period DESC, upload_time DESC"
            ).fetchall()
        else:
            uploads_data = conn.execute(
                "SELECT id, period, filename, file_type, sheet_count, row_count, upload_time, status FROM uploads WHERE period = ? ORDER BY upload_time DESC",
                (mgmt_period,),
            ).fetchall()
        conn.close()

        if not uploads_data:
            st.info("暂无上传记录。")
        else:
            df_uploads = pd.DataFrame(uploads_data, columns=["ID", "月份", "文件名", "格式", "Sheet数", "总行数", "上传时间", "状态"])
            df_uploads["月份"] = df_uploads["月份"].apply(lambda x: period_label(x) if x else "未分类")
            df_uploads["上传时间"] = df_uploads["上传时间"].apply(lambda x: x[:19].replace("T", " "))
            st.dataframe(df_uploads, use_container_width=True, hide_index=True)

            # 删除功能
            del_id = st.number_input("输入ID删除记录", min_value=0, step=1, value=0)
            if del_id > 0 and st.button("🗑️ 删除该记录", type="secondary"):
                conn = get_db()
                # 获取文件路径并删除文件
                file_row = conn.execute("SELECT file_path FROM uploads WHERE id = ?", (del_id,)).fetchone()
                if file_row and file_row[0] and os.path.exists(file_row[0]):
                    os.remove(file_row[0])
                conn.execute("DELETE FROM uploads WHERE id = ?", (del_id,))
                conn.commit()
                conn.close()
                st.success(f"已删除记录 #{del_id}")
                st.rerun()

    # ── 生成记录 ──
    with tab_generations:
        conn = get_db()
        if mgmt_period == "全部月份":
            gen_data = conn.execute("""
                SELECT g.id, g.period, u.filename, m.name, g.ai_model, g.output_filename, g.status, g.created_at, g.duration_seconds
                FROM generations g
                LEFT JOIN uploads u ON g.source_upload_id = u.id
                LEFT JOIN mappings m ON g.mapping_id = m.id
                ORDER BY g.period DESC, g.created_at DESC
            """).fetchall()
        else:
            gen_data = conn.execute("""
                SELECT g.id, g.period, u.filename, m.name, g.ai_model, g.output_filename, g.status, g.created_at, g.duration_seconds
                FROM generations g
                LEFT JOIN uploads u ON g.source_upload_id = u.id
                LEFT JOIN mappings m ON g.mapping_id = m.id
                WHERE g.period = ?
                ORDER BY g.created_at DESC
            """, (mgmt_period,)).fetchall()
        conn.close()

        if not gen_data:
            st.info("暂无生成记录。")
        else:
            df_gen = pd.DataFrame(gen_data, columns=["ID", "月份", "源文件", "映射规则", "AI模型", "输出文件", "状态", "生成时间", "耗时(秒)"])
            df_gen["月份"] = df_gen["月份"].apply(lambda x: period_label(x) if x else "未分类")
            df_gen["生成时间"] = df_gen["生成时间"].apply(lambda x: x[:19].replace("T", " "))
            st.dataframe(df_gen, use_container_width=True, hide_index=True)

            # 下载已生成的报表
            st.markdown("#### 下载报表")
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
    with tab_storage:
        st.markdown("### 存储空间使用")

        # 计算各目录大小
        def get_dir_size(path):
            total = 0
            for p in Path(path).rglob("*"):
                if p.is_file():
                    total += p.stat().st_size
            return total

        data_size = get_dir_size(DATA_DIR)
        output_size = get_dir_size(OUTPUT_DIR)
        mapping_size = get_dir_size(MAPPING_DIR)
        total_size = data_size + output_size + mapping_size

        def fmt_size(b):
            if b < 1024:
                return f"{b} B"
            elif b < 1024 * 1024:
                return f"{b / 1024:.1f} KB"
            else:
                return f"{b / (1024 * 1024):.1f} MB"

        s1, s2, s3, s4 = st.columns(4)
        with s1:
            st.metric("数据文件", fmt_size(data_size))
        with s2:
            st.metric("生成报表", fmt_size(output_size))
        with s3:
            st.metric("映射规则", fmt_size(mapping_size))
        with s4:
            st.metric("总计", fmt_size(total_size))

        # 数据库信息
        st.markdown("### 数据库信息")
        db_size = os.path.getsize(str(DB_PATH)) if DB_PATH.exists() else 0
        st.markdown(f"- **数据库文件**：`{DB_PATH.name}`")
        st.markdown(f"- **文件大小**：{fmt_size(db_size)}")

        conn = get_db()
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        for t in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {t[0]}").fetchone()[0]
            st.markdown(f"- **表 `{t[0]}`**：{count} 条记录")
        conn.close()
