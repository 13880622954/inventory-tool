import streamlit as st
import pandas as pd
from datetime import datetime
import io

# ========== 配置 ==========
TARGET_WAREHOUSES = [
    "北京库", "长春库", "长沙库新", "成都库", "福州库", "广州库", "贵阳库",
    "哈尔滨库", "杭州库", "合肥库", "呼市库", "济南库", "昆明库", "兰州库",
    "绵阳库", "南昌库", "南充库", "南京库", "南宁库", "沈阳库", "石家庄库",
    "太原库", "天津库", "武汉库", "乌鲁木齐库", "无锡库", "西安库", "郑州库", "重庆库"
]
EXCLUDE_STORAGE_LOC = ["HF0C", "N57S"]

st.set_page_config(page_title="库存需求数据处理", layout="wide")
st.title("📦 预留未清数据清洗与库位匹配")

# ---------- 文件上传区域 ----------
st.subheader("1. 请上传预留未清文件")
main_file = st.file_uploader("选择预留未清文件（Excel 或 CSV）", type=["xlsx", "xls", "csv"])

st.subheader("2. 上传库位对照表（可选）")
st.caption("如果预留未清文件中已包含名为“所有库位”的工作表，则无需单独上传。")
loc_file = st.file_uploader("选择库位对照表（Excel）", type=["xlsx", "xls"])

# ---------- 状态管理 ----------
if "process_clicked" not in st.session_state:
    st.session_state.process_clicked = False

def process_data(main_file, loc_file):
    """执行全部数据处理，返回清洗后的 DataFrame 或 None"""
    try:
        if main_file.name.endswith('.csv'):
            df_main = pd.read_csv(main_file)
        else:
            df_main = pd.read_excel(main_file, sheet_name=0)
    except Exception as e:
        st.error(f"❌ 读取文件失败：{e}")
        return None

    df_main.columns = df_main.columns.str.strip()
    required_cols = ["需求日期", "差额数量", "存储位置", "预留编号"]
    missing = [col for col in required_cols if col not in df_main.columns]
    if missing:
        st.error(f"❌ 文件缺少必需列：{missing}")
        return None

    # 读取库位对照表
    df_loc = None
    if loc_file is not None:
        try:
            df_loc = pd.read_excel(loc_file, sheet_name="所有库位")
        except Exception as e:
            st.warning(f"⚠️ 读取库位文件失败：{e}，尝试从预留未清文件中查找。")
    if df_loc is None and not main_file.name.endswith('.csv'):
        try:
            xl = pd.ExcelFile(main_file)
            if "所有库位" in xl.sheet_names:
                df_loc = pd.read_excel(main_file, sheet_name="所有库位")
            else:
                st.warning("⚠️ 预留未清文件中未找到“所有库位”工作表，且未单独上传库位文件。")
        except Exception as e:
            st.warning(f"⚠️ 检查工作表时出错：{e}")
    if df_loc is None:
        st.error("❌ 未成功加载库位对照表，无法进行库位匹配。")
        return None
    df_loc.columns = df_loc.columns.str.strip()
    if "库位" not in df_loc.columns or "仓库" not in df_loc.columns:
        st.error("❌ 库位对照表必须包含“库位”和“仓库”两列。")
        return None
    df_loc["库位"] = df_loc["库位"].astype(str).str.strip()
    df_loc["仓库"] = df_loc["仓库"].astype(str).str.strip()

    # 1. 删除指定存储位置
    invalid_mask = df_main["存储位置"].astype(str).str.strip().isin(EXCLUDE_STORAGE_LOC)
    df_main = df_main[~invalid_mask]

    # 2. 删除差额数量为 0
    df_main = df_main[df_main["差额数量"] != 0]

    # 3. 删除两个月内的需求日期
    df_main["需求日期"] = pd.to_datetime(df_main["需求日期"], errors="coerce")
    df_main = df_main.dropna(subset=["需求日期"])
    today = pd.Timestamp(datetime.now().date())
    two_months_ago = today - pd.DateOffset(months=2)
    two_months_later = today + pd.DateOffset(months=2)
    df_main = df_main[(df_main["需求日期"] < two_months_ago) | (df_main["需求日期"] > two_months_later)]

    # 4. 匹配库位描述
    col_store = "存储位置"
    store_idx = df_main.columns.get_loc(col_store)
    df_main.insert(store_idx + 1, "库位描述", pd.NA)
    df_main["_存储位置_clean"] = df_main[col_store].astype(str).str.strip()
    merged = df_main.merge(df_loc[["库位", "仓库"]], left_on="_存储位置_clean", right_on="库位", how="left")
    merged["库位描述"] = merged["仓库"]
    merged.drop(columns=["_存储位置_clean", "库位", "仓库"], inplace=True, errors="ignore")
    df_main = merged

    # 5. 仅保留目标仓库
    df_main["库位描述"] = df_main["库位描述"].astype(str).str.strip()
    df_main = df_main[df_main["库位描述"].isin(TARGET_WAREHOUSES)]

    df_main.reset_index(drop=True, inplace=True)
    return df_main

def build_summary(df):
    """生成库位汇总 DataFrame：列出全部29个库位，统计不重复预留编号个数，无数据留空"""
    # 统计每个库位描述的不重复预留编号个数
    stats = df.groupby("库位描述")["预留编号"].nunique().reset_index()
    stats.columns = ["库位描述", "预留单号数量"]

    # 构建包含所有目标库位的骨架
    all_warehouses_df = pd.DataFrame({"库位描述": TARGET_WAREHOUSES})

    # 左连接，缺失值填充为空白（NaN）
    summary = all_warehouses_df.merge(stats, on="库位描述", how="left")
    summary["预留单号数量"] = summary["预留单号数量"].apply(
        lambda x: "" if pd.isna(x) else int(x)
    )
    return summary

# ---------- 主界面逻辑 ----------
if main_file is not None:
    st.button("🚀 开始处理", type="primary", on_click=lambda: st.session_state.update(process_clicked=True))

    if st.session_state.process_clicked:
        with st.spinner("数据处理中，请稍候..."):
            result_df = process_data(main_file, loc_file)
        if result_df is not None:
            st.success(f"🎉 处理完成！最终行数：{len(result_df)}")
            st.subheader("📋 处理结果预览")
            st.dataframe(result_df)

            # 生成汇总表
            summary_df = build_summary(result_df)

            # 准备 Excel 下载（两个工作表）
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                result_df.to_excel(writer, index=False, sheet_name='清洗后数据')
                summary_df.to_excel(writer, index=False, sheet_name='库位汇总')
            output.seek(0)

            st.download_button(
                label="📥 下载处理后的 Excel 文件（含汇总）",
                data=output,
                file_name="processed_data.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
else:
    st.info("👆 请先上传预留未清文件，然后点击“开始处理”按钮。")
