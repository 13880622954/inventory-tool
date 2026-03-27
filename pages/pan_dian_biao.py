import streamlit as st
import pandas as pd
import io
import zipfile
import os
import tempfile
import warnings
warnings.filterwarnings('ignore')

# ========== 配置（与原脚本一致） ==========
SHEET_PHYSICAL = '实物库位表'
SHEET_GIFT = '赠品库位表'
INVENTORY_SHEET_PRODUCT = '成品'
INVENTORY_SHEET_GIFT = '赠品'
COL_LOCATION_CODE = '库位代码'
COL_LOCATION_DESC = '仓库描述'

FIXED_COLUMNS = [
    '工厂', '库位', '库位名称', '物料代码', '物料描述', '产品等级', '单位',
    'ERP账面数量', 'ERP账面金额', '入库未记数', '出库未记数', '调整后数量',
    '实盘数量', '盘盈（+）', '差异原因分析', '实物状态', '是否影响正常销售',
    '产品账实等级是否一致', '3个月库龄', '4-6个月库龄', '7-12个月库龄',
    '1-2年库龄', '2-3年库龄', '3年以上库龄', '10年以上库龄',
    '计提跌价准备金额', '实物状态是否为裸机', '库位描述'
]

SUM_COLUMNS = [
    'ERP账面数量', '入库未记数', '出库未记数', '调整后数量', '实盘数量', '盘盈（+）'
]

# ========== 辅助函数（从原脚本迁移，适配 BytesIO） ==========
def clean_str(val):
    if pd.isna(val):
        return ''
    return str(val).strip()

def extract_location_dict_from_bytes(file_bytes, sheet_name):
    """从库位表字节流提取映射 {库位代码: 仓库描述}"""
    try:
        df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name)
        if COL_LOCATION_CODE not in df.columns:
            st.error(f"{sheet_name} 中缺少列: {COL_LOCATION_CODE}")
            return {}
        location_dict = {}
        for _, row in df.iterrows():
            code = clean_str(row[COL_LOCATION_CODE])
            desc = clean_str(row[COL_LOCATION_DESC]) if COL_LOCATION_DESC in df.columns else code
            if code:
                location_dict[code] = desc
        return location_dict
    except Exception as e:
        st.error(f"读取 {sheet_name} 失败: {e}")
        return {}

def find_two_row_header(df):
    """找到表头所在的两行，返回 (header_top_idx, header_bottom_idx, data_start)"""
    header_bottom_idx = None
    for idx in range(min(50, len(df))):
        row = df.iloc[idx]
        row_str = ' '.join([clean_str(v) for v in row.values if pd.notna(v)])
        if '工厂' in row_str and '库位' in row_str:
            header_bottom_idx = idx
            break
    if header_bottom_idx is None:
        return None, None, None

    header_top_idx = header_bottom_idx - 1
    if header_top_idx < 0:
        header_top_idx = None

    data_start = None
    for j in range(header_bottom_idx + 1, len(df)):
        first_cell = clean_str(df.iloc[j, 0]) if df.shape[1] > 0 else ''
        if first_cell and '合计' not in first_cell:
            data_start = j
            break

    return header_top_idx, header_bottom_idx, data_start

def combine_two_row_header(df, header_top_idx, header_bottom_idx):
    """合并两行表头，返回唯一列名列表"""
    top_row = [clean_str(v) for v in df.iloc[header_top_idx].values] if header_top_idx is not None else []
    bottom_row = [clean_str(v) for v in df.iloc[header_bottom_idx].values]

    max_len = max(len(top_row), len(bottom_row))
    top_row += [''] * (max_len - len(top_row))
    bottom_row += [''] * (max_len - len(bottom_row))

    # 向前填充第一行（处理合并单元格）
    last_non_empty = ''
    for i in range(max_len):
        if top_row[i]:
            last_non_empty = top_row[i]
        else:
            top_row[i] = last_non_empty

    combined = []
    for i in range(max_len):
        top = top_row[i]
        bottom = bottom_row[i]
        if bottom:
            combined.append(f"{top}\n{bottom}" if top else bottom)
        else:
            combined.append(top)

    # 去重
    seen = {}
    unique = []
    for col in combined:
        if col in seen:
            seen[col] += 1
            new_col = f"{col}_{seen[col]}"
        else:
            seen[col] = 0
            new_col = col
        unique.append(new_col)
    return unique

def extract_matched_rows_from_bytes(file_bytes, sheet_name, location_dict):
    """
    从上传的盘点表文件字节流中提取指定 sheet，进行库位匹配，返回 DataFrame（包含原始所有列 + 库位代码 + 仓库描述）
    """
    try:
        df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name, header=None)
    except Exception as e:
        st.error(f"读取 {sheet_name} 失败: {e}")
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    header_top, header_bottom, data_start = find_two_row_header(df)
    if header_bottom is None or data_start is None:
        return pd.DataFrame()

    # 合并表头（仅用于识别库位列）
    headers = combine_two_row_header(df, header_top, header_bottom)

    # 提取数据行
    data_rows = []
    for idx in range(data_start, len(df)):
        row = df.iloc[idx]
        first_cell = clean_str(row.iloc[0]) if len(row) > 0 else ''
        if first_cell == '' or '合计' in first_cell:
            break
        data_rows.append(row.values)

    if not data_rows:
        return pd.DataFrame()

    # 构建 DataFrame，使用临时列名
    num_cols = len(data_rows[0])
    temp_columns = [f'col_{i}' for i in range(num_cols)]
    df_data = pd.DataFrame(data_rows, columns=temp_columns)

    # 找到库位列
    location_col_idx = None
    for idx, col_name in enumerate(headers):
        if '库位' in col_name and '库位名称' not in col_name:
            location_col_idx = idx
            break
    if location_col_idx is None:
        st.warning("未找到库位列")
        return pd.DataFrame()

    location_col = f'col_{location_col_idx}'
    df_data['库位代码'] = df_data[location_col].astype(str).str.strip()
    df_data['仓库描述'] = df_data['库位代码'].map(location_dict).fillna('')
    matched = df_data[df_data['仓库描述'] != ''].copy()
    # 保留库位代码，不删除
    # matched.drop('库位代码', axis=1, inplace=True)  # 注释掉，保留库位代码
    return matched

def process_uploaded_inventory_zip(zip_bytes, product_location_dict, gift_location_dict):
    """
    处理上传的 zip 压缩包，遍历其中所有 Excel 文件，提取成品和赠品数据，返回两个 DataFrame
    """
    all_product = []
    all_gift = []

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for file_name in z.namelist():
            # 跳过目录和临时文件
            if file_name.endswith('/') or file_name.startswith('~$'):
                continue
            if not (file_name.endswith('.xlsx') or file_name.endswith('.xls')):
                continue
            try:
                with z.open(file_name) as f:
                    file_bytes = f.read()
                # 成品
                product_data = extract_matched_rows_from_bytes(file_bytes, INVENTORY_SHEET_PRODUCT, product_location_dict)
                if not product_data.empty:
                    all_product.append(product_data)
                # 赠品
                gift_data = extract_matched_rows_from_bytes(file_bytes, INVENTORY_SHEET_GIFT, gift_location_dict)
                if not gift_data.empty:
                    all_gift.append(gift_data)
            except Exception as e:
                st.warning(f"处理文件 {file_name} 时出错: {e}")

    combined_product = pd.concat(all_product, ignore_index=True) if all_product else pd.DataFrame()
    combined_gift = pd.concat(all_gift, ignore_index=True) if all_gift else pd.DataFrame()
    return combined_product, combined_gift

def merge_with_old_result(detail_df, old_result_df, key_col='库位代码'):
    """
    将盘点表明细与老功能结果进行匹配，用老结果中的“仓库描述”覆盖明细中的对应字段。
    """
    if detail_df.empty or old_result_df.empty:
        return detail_df

    if key_col not in old_result_df.columns:
        st.warning(f"老功能结果中缺少列: {key_col}，无法匹配")
        return detail_df

    if key_col not in detail_df.columns:
        st.warning(f"明细数据中没有列: {key_col}，无法匹配")
        return detail_df

    # 去重（保留第一个）
    old_result_unique = old_result_df.drop_duplicates(subset=[key_col])

    # 左连接，添加后缀 '_old'
    merged = detail_df.merge(old_result_unique, on=key_col, how='left', suffixes=('', '_old'))

    # 用老结果中的“仓库描述”覆盖明细中的“仓库描述”（如果有）
    if '仓库描述' in merged.columns and '仓库描述_old' in merged.columns:
        merged['仓库描述'] = merged['仓库描述_old'].fillna(merged['仓库描述'])
        merged.drop('仓库描述_old', axis=1, inplace=True)

    return merged

def align_to_fixed_columns_with_desc(df, fixed_cols, desc_col_name='仓库描述'):
    """将 DataFrame 对齐到固定列名，最后列为'库位描述'"""
    if df.empty:
        return pd.DataFrame(columns=fixed_cols)
    if fixed_cols[-1] != '库位描述':
        raise ValueError("固定表头最后一个元素必须是'库位描述'")
    num_data_cols = len(df.columns) - 1  # 最后一个是仓库描述列
    num_fixed = len(fixed_cols) - 1
    result = pd.DataFrame(index=df.index, columns=fixed_cols)
    for i in range(min(num_data_cols, num_fixed)):
        result.iloc[:, i] = df.iloc[:, i]
    result['库位描述'] = df[desc_col_name]
    return result

def summarize_by_warehouse(df):
    """按仓库描述汇总数值列（简化版，不再依赖 fixed_cols 位置）"""
    if df.empty:
        return pd.DataFrame(columns=['仓库描述', 'ERP账面数_汇总', '入库未计数', '出库未记数', '调整后数量', '实盘', '盘盈', '盘亏'])
    if '仓库描述' not in df.columns:
        return pd.DataFrame()

    # 确保数值列为数字
    for col in SUM_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        else:
            df[col] = 0

    # 分组聚合
    group_cols = ['仓库描述']
    sum_cols = [col for col in SUM_COLUMNS if col in df.columns]
    grouped = df.groupby(group_cols)[sum_cols].sum().reset_index()

    # 拆分盘盈盘亏
    if '盘盈（+）' in grouped.columns:
        grouped['盘盈'] = grouped['盘盈（+）'].apply(lambda x: x if x > 0 else 0)
        grouped['盘亏'] = grouped['盘盈（+）'].apply(lambda x: abs(x) if x < 0 else 0)
        grouped.drop('盘盈（+）', axis=1, inplace=True)
    else:
        grouped['盘盈'] = 0
        grouped['盘亏'] = 0

    rename_map = {
        'ERP账面数量': 'ERP账面数_汇总',
        '入库未记数': '入库未计数',
        '出库未记数': '出库未记数',
        '调整后数量': '调整后数量',
        '实盘数量': '实盘'
    }
    grouped.rename(columns={k: v for k, v in rename_map.items() if k in grouped.columns}, inplace=True)

    final_cols = ['仓库描述', 'ERP账面数_汇总', '入库未计数', '出库未记数', '调整后数量', '实盘', '盘盈', '盘亏']
    for col in final_cols:
        if col not in grouped.columns:
            grouped[col] = 0
    return grouped[final_cols]

# ========== Streamlit 页面 ==========
st.set_page_config(page_title="盘点表汇总工具", layout="wide")
st.title("📦 盘点表汇总工具")

st.markdown("""
本工具用于批量处理盘点表文件（支持上传 ZIP 压缩包），根据库位表进行库位匹配，生成按仓库汇总的报表。
可选上传“2026年2月美菱IB00工厂盘存数据、账外物资汇总.xlsx”文件，如果上传则进行匹配（用库位代码关联，更新仓库描述）。
""")

# 侧边栏上传文件
st.sidebar.header("1. 上传必需文件")
location_file = st.sidebar.file_uploader("库位表（Excel，需包含'实物库位表'和'赠品库位表'两个sheet）", type=['xlsx'])
inventory_zip = st.sidebar.file_uploader("盘点表压缩包（ZIP，内含多个盘点表Excel文件）", type=['zip'])

st.sidebar.header("2. 可选匹配文件")
match_file = st.sidebar.file_uploader("2026年2月美菱IB00工厂盘存数据、账外物资汇总.xlsx（可选）", type=['xlsx'])

if st.sidebar.button("开始处理"):
    if not location_file:
        st.error("请先上传库位表文件")
        st.stop()
    if not inventory_zip:
        st.error("请先上传盘点表压缩包")
        st.stop()

    with st.spinner("正在处理，请稍候..."):
        # 1. 读取库位表映射
        location_bytes = location_file.read()
        product_location_dict = extract_location_dict_from_bytes(location_bytes, SHEET_PHYSICAL)
        gift_location_dict = extract_location_dict_from_bytes(location_bytes, SHEET_GIFT)

        if not product_location_dict:
            st.error("实物库位表为空或格式错误，请检查")
            st.stop()

        st.success(f"实物库位映射: {len(product_location_dict)} 个")
        st.success(f"赠品库位映射: {len(gift_location_dict)} 个")

        # 2. 处理盘点表压缩包
        product_detail, gift_detail = process_uploaded_inventory_zip(
            inventory_zip.getvalue(), product_location_dict, gift_location_dict
        )

        if product_detail.empty and gift_detail.empty:
            st.error("没有匹配到任何数据，请检查库位表和盘点表文件")
            st.stop()

        # 3. 如果提供了匹配文件，进行匹配
        if match_file:
            st.info("检测到匹配文件，正在加载...")
            try:
                match_df = pd.read_excel(match_file)
                st.success(f"匹配文件加载成功，共 {len(match_df)} 行")
                # 匹配成品
                if not product_detail.empty:
                    product_detail = merge_with_old_result(product_detail, match_df)
                # 匹配赠品
                if not gift_detail.empty:
                    gift_detail = merge_with_old_result(gift_detail, match_df)
            except Exception as e:
                st.error(f"读取匹配文件失败: {e}")
        else:
            st.info("未上传匹配文件，将只进行库位匹配和汇总")

        # 4. 汇总
        product_summary = summarize_by_warehouse(product_detail)
        gift_summary = summarize_by_warehouse(gift_detail)

        # 5. 明细对齐固定表头
        product_output = align_to_fixed_columns_with_desc(product_detail, FIXED_COLUMNS, '仓库描述')
        gift_output = align_to_fixed_columns_with_desc(gift_detail, FIXED_COLUMNS, '仓库描述')

        # 6. 展示结果
        st.subheader("汇总结果")
        if not product_summary.empty:
            st.write("**成品按仓库汇总**")
            st.dataframe(product_summary)
        if not gift_summary.empty:
            st.write("**赠品按仓库汇总**")
            st.dataframe(gift_summary)

        # 明细可折叠
        with st.expander("查看明细"):
            if not product_output.empty:
                st.write("**成品明细汇总**")
                st.dataframe(product_output)
            if not gift_output.empty:
                st.write("**赠品明细汇总**")
                st.dataframe(gift_output)

        # 7. 下载按钮
        st.subheader("下载结果")
        col1, col2 = st.columns(2)
        if not product_summary.empty:
            col1.download_button("下载成品汇总 (CSV)", product_summary.to_csv(index=False).encode('utf-8-sig'), "成品汇总.csv", "text/csv")
        if not gift_summary.empty:
            col2.download_button("下载赠品汇总 (CSV)", gift_summary.to_csv(index=False).encode('utf-8-sig'), "赠品汇总.csv", "text/csv")
        if not product_output.empty:
            col1.download_button("下载成品明细 (CSV)", product_output.to_csv(index=False).encode('utf-8-sig'), "成品明细.csv", "text/csv")
        if not gift_output.empty:
            col2.download_button("下载赠品明细 (CSV)", gift_output.to_csv(index=False).encode('utf-8-sig'), "赠品明细.csv", "text/csv")

        st.success("处理完成！")
