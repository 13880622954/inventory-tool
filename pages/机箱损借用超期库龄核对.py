import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="库龄与库位对账工具", layout="wide")
st.title("📊 库龄表与库位表匹配及汇总分析")

col1, col2 = st.columns(2)
with col1:
    stock_file = st.file_uploader("请上传【库龄表】Excel文件", type=["xlsx", "xls"], key="stock")
with col2:
    location_file = st.file_uploader("请上传【库位表】Excel文件（包含工作表「所有库位」）", type=["xlsx", "xls"], key="location")

if stock_file and location_file:
    # 读取库龄表（默认第一个sheet）
    try:
        df_stock = pd.read_excel(stock_file)
        st.success("库龄表读取成功")
    except Exception as e:
        st.error(f"库龄表读取失败：{e}")
        st.stop()

    # 读取库位表中的“所有库位”工作表
    try:
        df_location = pd.read_excel(location_file, sheet_name="所有库位")
        st.success("库位表「所有库位」工作表读取成功")
    except Exception as e:
        st.error(f"库位表读取失败，请确保文件中包含名为「所有库位」的工作表：{e}")
        st.stop()

    with st.expander("🔍 库龄表原始数据预览（前5行）"):
        st.dataframe(df_stock.head())
    with st.expander("🔍 库位表「所有库位」原始数据预览（前5行）"):
        st.dataframe(df_location.head())

    # 1. 匹配库位名称
    loc_col = None
    loc_name_col = None
    for col in df_location.columns:
        if "库位" in col and "名称" not in col:
            loc_col = col
        if "库位名称" in col or ("名称" in col and "库位" in col):
            loc_name_col = col
    if loc_col is None or loc_name_col is None:
        st.error("库位表「所有库位」中未找到“库位”列或“库位名称”列，请检查列名。")
        st.stop()

    mapping = df_location[[loc_col, loc_name_col]].drop_duplicates(subset=[loc_col])
    mapping.columns = ["库位", "库位名称"]
    df_stock = df_stock.merge(mapping, left_on="库存地", right_on="库位", how="left")
    df_stock.drop(columns=["库位"], inplace=True)

    cols = df_stock.columns.tolist()
    loc_idx = cols.index("库存地")
    loc_name_col_in_df = cols.pop(cols.index("库位名称"))
    cols.insert(loc_idx + 1, loc_name_col_in_df)
    df_stock = df_stock[cols]

    # ========== 修改点1：required 增加“3年以上” ==========
    required = ["1月", "2月", "3月", "4月", "5月", "6月", "7-12月", "13-18月", "19-24月", "2-3年", "3年以上"]
    for col in required:
        if col not in df_stock.columns:
            st.error(f"库龄表中缺少必需列：{col}")
            st.stop()

    # 2. 新增“2月内库龄”
    # 确保数值类型，填充空值
    for col in ["1月", "2月"]:
        df_stock[col] = pd.to_numeric(df_stock[col], errors='coerce').fillna(0)
    df_stock["2月内库龄"] = df_stock["1月"] + df_stock["2月"]

    # 列排序（将“2月内库龄”插在“2月”后面）
    col_list = df_stock.columns.tolist()
    feb_idx = col_list.index("2月")
    new_col = col_list.pop(col_list.index("2月内库龄"))
    col_list.insert(feb_idx + 1, new_col)
    df_stock = df_stock[col_list]

    # ========== 修改点2：upper_cols 增加“3年以上” ==========
    upper_cols = ["3月", "4月", "5月", "6月", "7-12月", "13-18月", "19-24月", "2-3年", "3年以上"]
    # 确保所有列都是数值，并填充0
    for col in upper_cols:
        if col in df_stock.columns:
            df_stock[col] = pd.to_numeric(df_stock[col], errors='coerce').fillna(0)
    # 3. 新增“2月以上库龄”
    df_stock["2月以上库龄"] = df_stock[upper_cols].sum(axis=1)

    with st.expander("📌 处理后的完整库龄表（前10行）"):
        st.dataframe(df_stock.head(10))

    # 4. 按库位名称+库存地汇总
    summary_df = df_stock.groupby(["库位名称", "库存地"], as_index=False)[["2月内库龄", "2月以上库龄"]].sum()

    st.subheader("📋 按【库位名称 + 库存地】汇总表")
    st.dataframe(summary_df, use_container_width=True)

    # 5. 按库存地尾数大汇总
    tail_mapping = {
        "4": "干线箱损库",
        "5": "配送箱损库",
        "8": "借用库",
        "2": "干线机损库",
        "3": "配送机损库"
    }

    def get_tail(loc):
        s = str(loc).strip()
        if not s:
            return None
        last_char = s[-1]
        return last_char if last_char.isdigit() else None

    summary_df["库存地尾数"] = summary_df["库存地"].apply(get_tail)
    filtered = summary_df[summary_df["库存地尾数"].isin(tail_mapping.keys())].copy()
    filtered["库名"] = filtered["库存地尾数"].map(tail_mapping)

    final_summary = filtered.groupby("库名", as_index=False)[["2月内库龄", "2月以上库龄"]].sum()
    # 补全缺失的库名
    for name in tail_mapping.values():
        if name not in final_summary["库名"].values:
            final_summary = pd.concat([final_summary, pd.DataFrame({"库名": [name], "2月内库龄": [0], "2月以上库龄": [0]})], ignore_index=True)

    st.subheader("🏭 按库存地尾数大汇总（最终报表）")
    st.dataframe(final_summary, use_container_width=True)

    # 6. 导出到一个 Excel 文件的两个工作表
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        summary_df.to_excel(writer, sheet_name="库位库存地汇总", index=False)
        final_summary.to_excel(writer, sheet_name="尾数大汇总", index=False)
    output.seek(0)

    st.subheader("💾 下载结果（一个Excel文件，包含两个工作表）")
    st.download_button(
        label="下载汇总结果.xlsx",
        data=output,
        file_name="库龄汇总结果.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    st.success("处理完成！")
