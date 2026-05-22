import streamlit as st
import pandas as pd
from datetime import datetime
import io

# ========== 全局配置 ==========
TARGET_WAREHOUSES = [
    "北京库", "长春库", "长沙库新", "成都库", "福州库", "广州库", "贵阳库",
    "哈尔滨库", "杭州库", "合肥库", "呼市库", "济南库", "昆明库", "兰州库",
    "绵阳库", "南昌库", "南充库", "南京库", "南宁库", "沈阳库", "石家庄库",
    "太原库", "天津库", "武汉库", "乌鲁木齐库", "无锡库", "西安库", "郑州库", "重庆库"
]
# 两个业务都需要排除的存储位置/库存地
EXCLUDE_STORAGE_LOC = ["HF0C", "N57S", "N22P"]

st.set_page_config(page_title="多类型数据清洗", layout="wide")
st.title("📦 预留未清 & 销售未清 数据清洗与库位匹配")

# ---------- 智能文件读取函数 ----------
def read_uploaded_file(uploaded_file, sheet_name=0):
    fname = uploaded_file.name
    if fname.endswith('.csv'):
        return pd.read_csv(uploaded_file)
    else:
        # 依次尝试不同引擎，兼容各种Excel格式
        for engine in [None, 'openpyxl', 'xlrd']:
            try:
                return pd.read_excel(uploaded_file, sheet_name=sheet_name, engine=engine)
            except Exception:
                continue
        raise ValueError("无法读取文件，请检查文件是否为有效的 Excel/CSV 格式。")

def get_excel_sheet_names(uploaded_file):
    fname = uploaded_file.name
    if fname.endswith('.csv'):
        return []
    for engine in [None, 'openpyxl', 'xlrd']:
        try:
            xl = pd.ExcelFile(uploaded_file, engine=engine)
            return xl.sheet_names
        except Exception:
            continue
    return []

# ---------- 文件上传区域 ----------
col1, col2 = st.columns(2)
with col1:
    st.subheader("📋 预留未清文件（可选）")
    resv_file = st.file_uploader("上传预留未清 Excel 或 CSV", type=["xlsx", "xls", "csv"], key="resv")
with col2:
    st.subheader("📋 销售未清文件（可选）")
    sales_file = st.file_uploader("上传销售未清 Excel 或 CSV", type=["xlsx", "xls", "csv"], key="sales")

st.subheader("🗂️ 库位对照表（共用，可选）")
st.caption("如果预留未清或销售未清文件中已包含“所有库位”工作表，则无需单独上传。否则请上传包含该工作表的 Excel 文件。")
loc_file = st.file_uploader("上传库位对照表（Excel）", type=["xlsx", "xls"], key="loc")

# ---------- 状态管理 ----------
if "process_clicked" not in st.session_state:
    st.session_state.process_clicked = False

# ========== 库位对照表加载 ==========
def load_location_mapping(main_file, loc_file):
    df_loc = None
    if loc_file is not None:
        try:
            df_loc = read_uploaded_file(loc_file, sheet_name="所有库位")
        except Exception as e:
            st.warning(f"⚠️ 读取单独库位文件失败：{e}")
    if df_loc is None and main_file is not None:
        try:
            sheets = get_excel_sheet_names(main_file)
            if "所有库位" in sheets:
                df_loc = read_uploaded_file(main_file, sheet_name="所有库位")
        except Exception as e:
            st.warning(f"⚠️ 从主数据文件中读取库位工作表失败：{e}")
    if df_loc is None:
        return None
    df_loc.columns = df_loc.columns.str.strip()
    if "库位" not in df_loc.columns or "仓库" not in df_loc.columns:
        st.error("❌ 库位对照表必须包含“库位”和“仓库”两列。")
        return None
    df_loc["库位"] = df_loc["库位"].astype(str).str.strip()
    df_loc["仓库"] = df_loc["仓库"].astype(str).str.strip()
    return df_loc

# ========== 预留未清处理 ==========
def process_resv_data(df_main, df_loc):
    required = ["需求日期", "差额数量", "存储位置", "预留编号"]
    missing = [c for c in required if c not in df_main.columns]
    if missing:
        st.error(f"❌ 预留未清文件缺少必需列：{missing}")
        return None

    # 1. 删除 HF0C / N57S
    mask = df_main["存储位置"].astype(str).str.strip().isin(EXCLUDE_STORAGE_LOC)
    df_main = df_main[~mask]

    # 2. 删除差额数量为0
    df_main = df_main[df_main["差额数量"] != 0]

    # 3. 删除两个月内的需求日期
    df_main["需求日期"] = pd.to_datetime(df_main["需求日期"], errors="coerce")
    df_main = df_main.dropna(subset=["需求日期"])
    today = pd.Timestamp(datetime.now().date())
    two_months_ago = today - pd.DateOffset(months=2)
    two_months_later = today + pd.DateOffset(months=2)
    df_main = df_main[(df_main["需求日期"] < two_months_ago) | (df_main["需求日期"] > two_months_later)]

    # 4. 匹配库位
    store_col = "存储位置"
    idx = df_main.columns.get_loc(store_col)
    df_main.insert(idx + 1, "库位描述", pd.NA)
    df_main["_key"] = df_main[store_col].astype(str).str.strip()
    merged = df_main.merge(df_loc[["库位", "仓库"]], left_on="_key", right_on="库位", how="left")
    merged["库位描述"] = merged["仓库"]
    merged.drop(columns=["_key", "库位", "仓库"], inplace=True, errors="ignore")
    df_main = merged

    # 5. 仅保留目标仓库
    df_main["库位描述"] = df_main["库位描述"].astype(str).str.strip()
    df_main = df_main[df_main["库位描述"].isin(TARGET_WAREHOUSES)]
    df_main.reset_index(drop=True, inplace=True)
    return df_main

def build_resv_summary(df):
    stats = df.groupby("库位描述")["预留编号"].nunique().reset_index()
    stats.columns = ["库位描述", "预留单号数量"]
    all_wh = pd.DataFrame({"库位描述": TARGET_WAREHOUSES})
    summary = all_wh.merge(stats, on="库位描述", how="left")
    summary["预留单号数量"] = summary["预留单号数量"].apply(lambda x: "" if pd.isna(x) else int(x))
    return summary

# ========== 销售未清处理 ==========
def process_sales_data(df_main, df_loc):
    required = ["库存地", "交货号"]
    missing = [c for c in required if c not in df_main.columns]
    if missing:
        st.error(f"❌ 销售未清文件缺少必需列：{missing}")
        return None

    # 1. 删除 HF0C / N57S （新增规则）
    mask = df_main["库存地"].astype(str).str.strip().isin(EXCLUDE_STORAGE_LOC)
    df_main = df_main[~mask]

    # 2. 匹配库位
    store_col = "库存地"
    idx = df_main.columns.get_loc(store_col)
    df_main.insert(idx + 1, "库位描述", pd.NA)
    df_main["_key"] = df_main[store_col].astype(str).str.strip()
    merged = df_main.merge(df_loc[["库位", "仓库"]], left_on="_key", right_on="库位", how="left")
    merged["库位描述"] = merged["仓库"]
    merged.drop(columns=["_key", "库位", "仓库"], inplace=True, errors="ignore")
    df_main = merged

    # 3. 仅保留目标仓库
    df_main["库位描述"] = df_main["库位描述"].astype(str).str.strip()
    df_main = df_main[df_main["库位描述"].isin(TARGET_WAREHOUSES)]
    df_main.reset_index(drop=True, inplace=True)
    return df_main

def build_sales_summary(df):
    stats = df.groupby("库位描述")["交货号"].nunique().reset_index()
    stats.columns = ["库位描述", "交货单数量"]
    all_wh = pd.DataFrame({"库位描述": TARGET_WAREHOUSES})
    summary = all_wh.merge(stats, on="库位描述", how="left")
    summary["交货单数量"] = summary["交货单数量"].apply(lambda x: "" if pd.isna(x) else int(x))
    return summary

# ========== 主界面逻辑 ==========
has_resv = resv_file is not None
has_sales = sales_file is not None
has_any = has_resv or has_sales

if has_any:
    st.button("🚀 开始处理", type="primary", on_click=lambda: st.session_state.update(process_clicked=True))

    if st.session_state.process_clicked:
        main_for_loc = resv_file if has_resv else sales_file
        df_loc = load_location_mapping(main_for_loc, loc_file)
        if df_loc is None:
            st.error("❌ 无法加载库位对照表，请确保上传了包含“所有库位”工作表的 Excel 文件。")
            st.stop()

        # 处理预留未清
        if has_resv:
            with st.spinner("正在处理预留未清数据..."):
                try:
                    df_resv = read_uploaded_file(resv_file)
                    df_resv = process_resv_data(df_resv, df_loc)
                except Exception as e:
                    st.error(f"❌ 处理预留未清文件失败：{e}")
                    df_resv = None
            if df_resv is not None:
                st.success(f"✅ 预留未清处理完成，共 {len(df_resv)} 行")
                st.subheader("📋 预留未清结果预览")
                st.dataframe(df_resv)
                summary_resv = build_resv_summary(df_resv)
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df_resv.to_excel(writer, index=False, sheet_name='预留未清数据')
                    summary_resv.to_excel(writer, index=False, sheet_name='预留未清汇总')
                output.seek(0)
                st.download_button(
                    label="📥 下载预留未清结果（含汇总）",
                    data=output,
                    file_name="预留未清_processed.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                st.markdown("---")

        # 处理销售未清
        if has_sales:
            with st.spinner("正在处理销售未清数据..."):
                try:
                    df_sales = read_uploaded_file(sales_file)
                    df_sales = process_sales_data(df_sales, df_loc)
                except Exception as e:
                    st.error(f"❌ 处理销售未清文件失败：{e}")
                    df_sales = None
            if df_sales is not None:
                st.success(f"✅ 销售未清处理完成，共 {len(df_sales)} 行")
                st.subheader("📋 销售未清结果预览")
                st.dataframe(df_sales)
                summary_sales = build_sales_summary(df_sales)
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df_sales.to_excel(writer, index=False, sheet_name='销售未清数据')
                    summary_sales.to_excel(writer, index=False, sheet_name='销售未清汇总')
                output.seek(0)
                st.download_button(
                    label="📥 下载销售未清结果（含汇总）",
                    data=output,
                    file_name="销售未清_processed.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                st.markdown("---")
else:
    st.info("👆 请上传预留未清文件、销售未清文件中的至少一个，然后点击“开始处理”。")
