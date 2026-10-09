import streamlit as st
import pandas as pd
import zipfile
import io
import warnings
from openpyxl import load_workbook, Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from datetime import datetime
from collections import Counter

warnings.filterwarnings('ignore')

# ========== 配置 ==========
SHEET_PHYSICAL = '实物库位表'
SHEET_GIFT = '赠品库位表'
COL_LOCATION_CODE = '库位代码'
COL_LOCATION_DESC = '仓库描述'

FIXED_COLUMNS = [
    '工厂', '库位', '库位名称', '物料代码', '物料描述', '产品等级', '单位',
    'ERP账面数量', 'ERP账面金额', '入库未记数', '出库未记数', '调整后数量',
    '实盘数量', '盘盈（+）', '差异原因分析', '实物状态', '是否影响正常销售',
    '产品账实等级是否一致', '3个月库龄', '4-6个月库龄', '7-12个月库龄',
    '1-2年库龄', '2-3年库龄', '3年以上库龄', '10年以上库龄',
    '计提跌价准备金额', '实物状态是否为裸机'
]

SUM_COLUMNS = [
    'ERP账面数量', '入库未记数', '出库未记数', '调整后数量', '实盘数量', '盘盈（+）'
]

if 'cached_files' not in st.session_state:
    st.session_state.cached_files = []

def clean_str(val):
    if pd.isna(val):
        return ''
    return str(val).strip()

def normalize(s):
    return s.replace('\n', '').replace(' ', '').replace('（', '').replace('）', '')

def ultra_clean(s):
    if pd.isna(s):
        return ''
    return ''.join(str(s).split()).upper()

def extract_location_dict_from_bytes(file_bytes, sheet_name):
    try:
        df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name)
        if COL_LOCATION_CODE not in df.columns:
            st.error(f"{sheet_name} 中缺少列: {COL_LOCATION_CODE}")
            return {}
        desc_col = COL_LOCATION_DESC if COL_LOCATION_DESC in df.columns else COL_LOCATION_CODE
        location_dict = {}
        for _, row in df.iterrows():
            code = ultra_clean(row[COL_LOCATION_CODE])
            desc = clean_str(row[desc_col]) if desc_col in df.columns else code
            if code:
                location_dict[code] = desc
        return location_dict
    except Exception as e:
        st.error(f"读取 {sheet_name} 失败: {e}")
        return {}

def sheet_has_total(file_bytes, sheet_name):
    try:
        df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name, header=None)
        all_text = ''.join(df.astype(str).values.flatten())
        return '合计' in all_text or '总计' in all_text
    except Exception:
        return False

def find_two_row_header(df):
    for idx in range(min(50, len(df))):
        row_str = ' '.join([normalize(clean_str(v)) for v in df.iloc[idx].values if pd.notna(v)])
        if 'ERP账面数量' in row_str and '入库未记数' in row_str and '实盘数量' not in row_str:
            header_bottom = idx
            header_top = idx - 1 if idx > 0 else None
            if header_top is not None:
                top_str = ' '.join([clean_str(v) for v in df.iloc[header_top].values if pd.notna(v)])
                if not ('物料代码' in top_str or '库位' in top_str or '工厂' in top_str):
                    header_top = None
            data_start = header_bottom + 1
            for j in range(data_start, min(data_start + 5, len(df))):
                cells = [clean_str(v) for v in df.iloc[j].values if pd.notna(v)]
                if cells and not any('合计' in c or '总计' in c for c in cells):
                    data_start = j
                    break
            return header_top, header_bottom, data_start
    for idx in range(min(50, len(df))):
        row_str = ' '.join([normalize(clean_str(v)) for v in df.iloc[idx].values if pd.notna(v)])
        if 'ERP账面数量' in row_str and '实盘数量' not in row_str:
            header_bottom = idx
            header_top = idx - 1 if idx > 0 else None
            if header_top is not None:
                top_str = ' '.join([clean_str(v) for v in df.iloc[header_top].values if pd.notna(v)])
                if not ('物料代码' in top_str or '库位' in top_str or '工厂' in top_str):
                    header_top = None
            data_start = header_bottom + 1
            for j in range(data_start, min(data_start + 5, len(df))):
                cells = [clean_str(v) for v in df.iloc[j].values if pd.notna(v)]
                if cells and not any('合计' in c or '总计' in c for c in cells):
                    data_start = j
                    break
            return header_top, header_bottom, data_start
    for idx in range(min(50, len(df))):
        row_str = ' '.join([normalize(clean_str(v)) for v in df.iloc[idx].values if pd.notna(v)])
        if ('ERP账面数量' in row_str or '入库未记数' in row_str or 
            '出库未记数' in row_str or '调整后数量' in row_str):
            header_bottom = idx
            header_top = idx - 1 if idx > 0 else None
            if header_top is not None:
                top_str = ' '.join([clean_str(v) for v in df.iloc[header_top].values if pd.notna(v)])
                if not ('物料代码' in top_str or '库位' in top_str or '工厂' in top_str):
                    header_top = None
            data_start = header_bottom + 1
            for j in range(data_start, min(data_start + 5, len(df))):
                cells = [clean_str(v) for v in df.iloc[j].values if pd.notna(v)]
                if cells and not any('合计' in c or '总计' in c for c in cells):
                    data_start = j
                    break
            return header_top, header_bottom, data_start
    for idx in range(min(50, len(df))):
        row_str = ' '.join([clean_str(v) for v in df.iloc[idx].values if pd.notna(v)])
        if '物料代码' in row_str and '库位' in row_str:
            return None, idx, idx + 1
    return None, None, None

def combine_two_row_header(df, header_top, header_bottom):
    if header_top is None:
        return [clean_str(v) for v in df.iloc[header_bottom].values]
    top_raw = df.iloc[header_top].values.tolist()
    bottom_raw = df.iloc[header_bottom].values.tolist()
    max_len = max(len(top_raw), len(bottom_raw))
    top_raw += [None] * (max_len - len(top_raw))
    bottom_raw += [None] * (max_len - len(bottom_raw))
    top_row = [clean_str(v) for v in top_raw]
    bottom_row = [clean_str(v) for v in bottom_raw]
    last_non_empty = ''
    filled_top = []
    for cell in top_row:
        if cell:
            last_non_empty = cell
        filled_top.append(last_non_empty)
    combined = []
    for up, low in zip(filled_top, bottom_row):
        if low:
            combined.append(f"{up}\n{low}" if up else low)
        else:
            combined.append(up)
    seen = {}
    unique = []
    for col in combined:
        if col in seen:
            seen[col] += 1
            unique.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            unique.append(col)
    return unique

def parse_value(val):
    try:
        return float(val) if pd.notna(val) else 0.0
    except (ValueError, TypeError):
        return 0.0

def extract_matched_rows_from_bytes(file_bytes, sheet_name, location_dict,
                                    file_name='',
                                    empty_warnings=None,
                                    product_empty_list=None,
                                    gift_empty_list=None,
                                    summary_rows_list=None,
                                    diagnostics=None,
                                    unmatched_codes=None):
    try:
        df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name, header=None)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()

    header_top, header_bottom, data_start = find_two_row_header(df)
    if header_bottom is None or data_start is None:
        if diagnostics is not None:
            diagnostics.append(f"{file_name}/{sheet_name}: 未找到表头")
        return pd.DataFrame()
    headers = combine_two_row_header(df, header_top, header_bottom)

    if diagnostics is not None:
        diagnostics.append(f"{file_name}/{sheet_name}: top={header_top}, bottom={header_bottom}, 合并后列数={len(headers)}")

    factory_col_idx = None
    location_col_idx = None
    mat_code_idx = None
    for i, h in enumerate(headers):
        if '工厂' in h:
            factory_col_idx = i
        if '库位' in h and '库位名称' not in h:
            location_col_idx = i
        if '物料代码' in h:
            mat_code_idx = i
    if location_col_idx is None:
        for i, h in enumerate(headers):
            if '库位' in h:
                location_col_idx = i
                break

    data_rows = []
    explicit_summary = False

    for idx in range(data_start, len(df)):
        row = df.iloc[idx]
        cells = [clean_str(v) for v in row.values if pd.notna(v)]
        if not cells:
            continue

        is_summary = False
        if any('合计' in c or '总计' in c for c in cells):
            is_summary = True
        if not is_summary and factory_col_idx is not None:
            factory_val = clean_str(row.iloc[factory_col_idx]) if factory_col_idx < len(row) else ''
            if '合计' in factory_val or '总计' in factory_val:
                is_summary = True
        if not is_summary and location_col_idx is not None and mat_code_idx is not None:
            loc_val = clean_str(row.iloc[location_col_idx]) if location_col_idx < len(row) else ''
            mat_val = clean_str(row.iloc[mat_code_idx]) if mat_code_idx < len(row) else ''
            if loc_val == '' and mat_val == '':
                has_num = False
                for v in row.values:
                    try:
                        if pd.notna(v) and float(v) != 0:
                            has_num = True
                            break
                    except (ValueError, TypeError):
                        pass
                if has_num:
                    is_summary = True

        if is_summary:
            if summary_rows_list is not None:
                sum_dict = {'文件名': file_name, 'Sheet': sheet_name}
                col_indices = {}
                for col_name in SUM_COLUMNS:
                    for i, h in enumerate(headers):
                        if col_name in h.replace('\n', ''):
                            col_indices[col_name] = i
                            break
                for col_name, cidx in col_indices.items():
                    if cidx < len(row.values):
                        sum_dict[col_name] = parse_value(row.values[cidx])
                    else:
                        sum_dict[col_name] = 0.0
                summary_rows_list.append(sum_dict)
            explicit_summary = True
            continue
        data_rows.append(row.values)

    if not data_rows:
        return pd.DataFrame()

    max_cols = max(len(r) for r in data_rows)
    data_rows = [list(r) + [None] * (max_cols - len(r)) for r in data_rows]
    num_cols = max_cols
    temp_columns = [f'col_{i}' for i in range(num_cols)]
    df_data = pd.DataFrame(data_rows, columns=temp_columns)

    if location_col_idx is None:
        for i, h in enumerate(headers):
            if i < num_cols and '库位' in h and '库位名称' not in h:
                location_col_idx = i
                break
        if location_col_idx is None:
            for i, h in enumerate(headers):
                if i < num_cols and '库位' in h:
                    location_col_idx = i
                    break
    if mat_code_idx is None:
        for i, h in enumerate(headers):
            if i < num_cols and '物料代码' in h:
                mat_code_idx = i
                break

    if not explicit_summary and mat_code_idx is not None and location_col_idx is not None:
        last_row = df_data.iloc[-1]
        loc_val = clean_str(last_row.iloc[location_col_idx]) if location_col_idx < num_cols else ''
        mat_val = clean_str(last_row.iloc[mat_code_idx]) if mat_code_idx < num_cols else ''
        if loc_val == '' and mat_val == '':
            if summary_rows_list is not None:
                sum_dict = {'文件名': file_name, 'Sheet': sheet_name}
                col_indices = {}
                for col_name in SUM_COLUMNS:
                    for i, h in enumerate(headers):
                        if col_name in h.replace('\n', ''):
                            col_indices[col_name] = i
                            break
                for col_name, cidx in col_indices.items():
                    if cidx < num_cols:
                        sum_dict[col_name] = parse_value(last_row.iloc[cidx])
                    else:
                        sum_dict[col_name] = 0.0
                summary_rows_list.append(sum_dict)
            df_data = df_data.iloc[:-1]

    if df_data.empty:
        return pd.DataFrame()

    location_col = df_data.columns[location_col_idx]
    raw_codes = df_data[location_col].astype(str).str.strip()
    df_data['库位代码'] = raw_codes.apply(ultra_clean)
    if unmatched_codes is not None:
        for code in df_data['库位代码'].unique():
            if code and code not in location_dict:
                unmatched_codes.add((file_name, sheet_name, code))

    is_empty_loc = (raw_codes == '') | (raw_codes.str.lower() == 'nan') | (raw_codes.isna())
    if is_empty_loc.any() and mat_code_idx is not None:
        mat_col = df_data.columns[mat_code_idx]
        mat_not_empty = ~df_data[mat_col].isna() & (df_data[mat_col].astype(str).str.strip() != '') & (df_data[mat_col].astype(str).str.strip().str.lower() != 'nan')
        real_empty = is_empty_loc & mat_not_empty
        if real_empty.any():
            empty_df = df_data[real_empty]
            if empty_warnings is not None:
                for idx in empty_df.index:
                    mat_code = clean_str(empty_df.loc[idx, mat_col])
                    excel_row = data_start + idx + 1
                    empty_warnings.append({
                        '文件名': file_name,
                        'Sheet': sheet_name,
                        'Excel行号': excel_row,
                        '物料代码': mat_code,
                        '物料描述': ''
                    })
            for _, row in empty_df.iterrows():
                vals = {}
                col_indices = {}
                for col_name in SUM_COLUMNS:
                    for i, h in enumerate(headers):
                        if col_name in h.replace('\n', ''):
                            col_indices[col_name] = i
                            break
                for col_name, cidx in col_indices.items():
                    if cidx < num_cols:
                        vals[col_name] = parse_value(row.iloc[cidx])
                    else:
                        vals[col_name] = 0.0
                if '赠品' in sheet_name:
                    if gift_empty_list is not None:
                        gift_empty_list.append(vals)
                else:
                    if product_empty_list is not None:
                        product_empty_list.append(vals)

    df_data['仓库描述'] = df_data['库位代码'].map(location_dict).fillna('')
    matched = df_data[df_data['仓库描述'] != ''].copy()
    matched.drop('库位代码', axis=1, inplace=True)
    return matched

def process_inventory_zip(zip_bytes, product_location_dict, gift_location_dict,
                          missing_total_list=None):
    all_product = []
    all_gift = []
    empty_warnings = []
    product_empty_list = []
    gift_empty_list = []
    summary_rows_list = []
    diagnostics = []
    unmatched_codes = set()

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for file_name in z.namelist():
            if file_name.endswith('/') or file_name.startswith('~$'):
                continue
            if not (file_name.endswith('.xlsx') or file_name.endswith('.xls')):
                continue
            try:
                with z.open(file_name) as f:
                    file_bytes = f.read()
                xl = pd.ExcelFile(io.BytesIO(file_bytes))
                sheets = xl.sheet_names
                product_sheet = next((s for s in sheets if '成品' in s or '正品' in s), None)
                gift_sheet = next((s for s in sheets if '赠品' in s), None)

                if missing_total_list is not None:
                    if product_sheet and not sheet_has_total(file_bytes, product_sheet):
                        missing_total_list.append((file_name, product_sheet, '成品'))
                    if gift_sheet and not sheet_has_total(file_bytes, gift_sheet):
                        missing_total_list.append((file_name, gift_sheet, '赠品'))

                if product_sheet:
                    product_data = extract_matched_rows_from_bytes(
                        file_bytes, product_sheet, product_location_dict,
                        file_name=file_name,
                        empty_warnings=empty_warnings,
                        product_empty_list=product_empty_list,
                        gift_empty_list=gift_empty_list,
                        summary_rows_list=summary_rows_list,
                        diagnostics=diagnostics,
                        unmatched_codes=unmatched_codes
                    )
                    if not product_data.empty:
                        all_product.append(product_data)

                if gift_sheet:
                    gift_data = extract_matched_rows_from_bytes(
                        file_bytes, gift_sheet, gift_location_dict,
                        file_name=file_name,
                        empty_warnings=empty_warnings,
                        product_empty_list=product_empty_list,
                        gift_empty_list=gift_empty_list,
                        summary_rows_list=summary_rows_list,
                        diagnostics=diagnostics,
                        unmatched_codes=unmatched_codes
                    )
                    if not gift_data.empty:
                        all_gift.append(gift_data)

            except Exception as e:
                st.warning(f"处理文件 {file_name} 出错: {e}")

    combined_product = pd.concat(all_product, ignore_index=True) if all_product else pd.DataFrame()
    combined_gift = pd.concat(all_gift, ignore_index=True) if all_gift else pd.DataFrame()
    return combined_product, combined_gift, empty_warnings, product_empty_list, gift_empty_list, summary_rows_list, diagnostics, unmatched_codes

def summarize_by_warehouse(df):
    if df.empty:
        return pd.DataFrame(columns=['仓库描述', 'ERP账面数_汇总', '入库未计数', '出库未记数', '调整后数量', '实盘', '盘盈', '盘亏'])
    if '仓库描述' not in df.columns:
        return pd.DataFrame()

    sum_indices = []
    for col in SUM_COLUMNS:
        if col in FIXED_COLUMNS:
            idx = FIXED_COLUMNS.index(col)
            if idx < len(df.columns) - 1:
                sum_indices.append(idx)

    for idx in sum_indices:
        col_name = f'col_{idx}'
        df[col_name] = pd.to_numeric(df.iloc[:, idx], errors='coerce').fillna(0)

    grouped = df.groupby('仓库描述')
    agg_dict = {f'col_{idx}': 'sum' for idx in sum_indices}
    result = grouped.agg(agg_dict).reset_index()

    rename_map = {}
    for orig, target in zip([f'col_{idx}' for idx in sum_indices], SUM_COLUMNS):
        rename_map[orig] = target
    result.rename(columns=rename_map, inplace=True)

    if '盘盈（+）' in result.columns:
        result['盘盈'] = result['盘盈（+）'].apply(lambda x: x if x > 0 else 0)
        result['盘亏'] = result['盘盈（+）'].apply(lambda x: abs(x) if x < 0 else 0)
        result.drop('盘盈（+）', axis=1, inplace=True)
    else:
        result['盘盈'] = 0
        result['盘亏'] = 0

    final_rename = {
        'ERP账面数量': 'ERP账面数_汇总',
        '入库未记数': '入库未计数',
        '出库未记数': '出库未记数',
        '调整后数量': '调整后数量',
        '实盘数量': '实盘'
    }
    for old, new in final_rename.items():
        if old in result.columns:
            result.rename(columns={old: new}, inplace=True)

    final_cols = ['仓库描述', 'ERP账面数_汇总', '入库未计数', '出库未记数', '调整后数量', '实盘', '盘盈', '盘亏']
    for col in final_cols:
        if col not in result.columns:
            result[col] = 0
    return result[final_cols]

def align_to_fixed_columns(df, fixed_cols):
    if df.empty:
        return pd.DataFrame(columns=fixed_cols + ['仓库描述'])
    num_data_cols = len(df.columns) - 1
    num_fixed = len(fixed_cols)
    result = pd.DataFrame(index=df.index, columns=fixed_cols + ['仓库描述'])
    for i in range(min(num_data_cols, num_fixed)):
        result.iloc[:, i] = df.iloc[:, i]
    result['仓库描述'] = df['仓库描述'].values if '仓库描述' in df.columns else ''
    return result

def update_match_file(match_file_bytes, product_summary, gift_summary):
    if match_file_bytes is None:
        return None
    try:
        wb = load_workbook(io.BytesIO(match_file_bytes))
    except Exception as e:
        st.error(f"无法加载匹配文件: {e}")
        return None

    product_sheet_name = None
    gift_sheet_name = None
    for sheet in wb.sheetnames:
        if '成品' in sheet:
            product_sheet_name = sheet
        elif '赠品' in sheet:
            gift_sheet_name = sheet

    def update_sheet(ws, summary_df, sheet_type):
        if summary_df.empty:
            return
        header_row = 2
        warehouse_col = None
        for col in range(1, ws.max_column + 1):
            if ws.cell(row=header_row, column=col).value == '仓库描述':
                warehouse_col = col
                break
        if warehouse_col is None:
            st.warning(f"{sheet_type} sheet 中未找到仓库描述列，跳过更新")
            return

        new_col_name = "ERP账面数量-明细汇总"
        new_col_idx = None
        for col in range(1, ws.max_column + 1):
            if ws.cell(row=header_row, column=col).value == new_col_name:
                new_col_idx = col
                break
        if new_col_idx is None:
            new_col_idx = ws.max_column + 1
            ws.cell(row=header_row, column=new_col_idx, value=new_col_name)

        col_updates = {
            'ERP账面数_汇总': new_col_name,
            '入库未计数': '入库未计数',
            '出库未记数': '出库未记数',
            '实盘': '实盘',
            '盘盈': '盘盈',
            '盘亏': '盘亏'
        }
        target_cols = {}
        for sum_field, target_name in col_updates.items():
            for col in range(1, ws.max_column + 1):
                if ws.cell(row=header_row, column=col).value == target_name:
                    target_cols[sum_field] = col
                    break

        summary_dict = {row['仓库描述']: row for _, row in summary_df.iterrows()}
        for row in range(3, ws.max_row + 1):
            warehouse = ws.cell(row=row, column=warehouse_col).value
            if warehouse is None:
                continue
            warehouse_str = str(warehouse).strip()
            if warehouse_str in summary_dict:
                for sum_field, col_idx in target_cols.items():
                    ws.cell(row=row, column=col_idx, value=summary_dict[warehouse_str][sum_field])

    if product_sheet_name:
        update_sheet(wb[product_sheet_name], product_summary, "成品")
    if gift_sheet_name:
        update_sheet(wb[gift_sheet_name], gift_summary, "赠品")

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output

def safe_write_excel(sheet_dict):
    wb = Workbook()
    if 'Sheet' in wb.sheetnames:
        del wb['Sheet']
    for name, df in sheet_dict.items():
        if df.empty:
            continue
        ws = wb.create_sheet(title=name)
        for r in dataframe_to_rows(df, index=False, header=True):
            ws.append(r)
    if not wb.sheetnames:
        wb.create_sheet("empty")
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output

def add_to_cache(file_bytes, original_filename):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = original_filename.rsplit('.', 1)[0]
    new_name = f"{base_name}_更新_{timestamp}.xlsx"
    st.session_state.cached_files.append({'name': new_name, 'data': file_bytes})
    st.success(f"已缓存: {new_name} (共 {len(st.session_state.cached_files)} 个)")

def clear_cache():
    st.session_state.cached_files = []
    st.success("缓存已清空")

def download_all_cache_as_zip():
    if not st.session_state.cached_files:
        return None
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        for item in st.session_state.cached_files:
            zf.writestr(item['name'], item['data'])
    zip_buffer.seek(0)
    return zip_buffer

st.set_page_config(page_title="盘点表汇总工具", layout="wide")
st.title("📊 盘点表批量汇总工具（最终完美版）")

st.markdown("""
**使用说明**：
1. 上传库位表（Excel，含“实物库位表”和“赠品库位表”两个sheet）。
2. 上传盘点表压缩包（ZIP，内含多个Excel文件）。
3. 可选上传匹配文件（Excel）。
4. 点击“开始处理”或“重新处理”执行。
""")

with st.sidebar:
    st.header("1. 上传必需文件")
    location_file = st.file_uploader("库位表 (Excel)", type=['xlsx'])
    inventory_zip = st.file_uploader("盘点表压缩包 (ZIP)", type=['zip'])
    st.header("2. 可选匹配文件")
    match_file = st.file_uploader("匹配文件 (Excel)", type=['xlsx'])

    st.header("3. 执行操作")
    col_btn1, col_btn2 = st.columns(2)
    with col_btn1:
        process_btn = st.button("开始处理")
    with col_btn2:
        re_process_btn = st.button("重新处理")

    st.header("📦 缓存区")
    if st.button("清空缓存"):
        clear_cache()
    if st.button("📥 打包下载所有缓存"):
        zip_data = download_all_cache_as_zip()
        if zip_data:
            st.download_button("点击下载所有缓存", zip_data, file_name="所有月份更新文件.zip", mime="application/zip")
        else:
            st.info("暂无缓存文件")
    if st.session_state.cached_files:
        st.write(f"已缓存 {len(st.session_state.cached_files)} 个文件")
        for item in st.session_state.cached_files:
            st.text(item['name'])

def run_processing(location_file, inventory_zip, match_file):
    if not location_file or not inventory_zip:
        st.error("请同时上传库位表和盘点表压缩包")
        return

    with st.spinner("正在处理..."):
        location_bytes = location_file.read()
        product_location_dict = extract_location_dict_from_bytes(location_bytes, SHEET_PHYSICAL)
        gift_location_dict = extract_location_dict_from_bytes(location_bytes, SHEET_GIFT)
        if not product_location_dict:
            st.error("实物库位表为空或格式错误")
            return
        st.success(f"实物库位映射: {len(product_location_dict)} 个")
        st.success(f"赠品库位映射: {len(gift_location_dict)} 个")

        missing_total = []
        product_detail, gift_detail, empty_warnings, product_empty, gift_empty, summary_rows, diagnostics, unmatched_codes = process_inventory_zip(
            inventory_zip.getvalue(), product_location_dict, gift_location_dict,
            missing_total_list=missing_total
        )
        st.write(f"成品明细行数: {len(product_detail)}")
        st.write(f"赠品明细行数: {len(gift_detail)}")

        if missing_total:
            with st.expander("❌ 缺少“合计/总计”字样的Sheet（请补充）"):
                df_missing = pd.DataFrame(missing_total, columns=['文件名', 'Sheet名', '类型'])
                st.dataframe(df_missing)
        else:
            st.success("✅ 所有成品/赠品Sheet均包含“合计”或“总计”字样")

        with st.expander("🔍 诊断信息"):
            if diagnostics:
                for d in diagnostics:
                    st.text(d)
            else:
                st.text("无诊断信息")

        if unmatched_codes:
            with st.expander("⚠️ 未匹配库位代码（请补充到库位表）"):
                df_un = pd.DataFrame(list(unmatched_codes), columns=['文件名', 'Sheet', '库位代码'])
                st.dataframe(df_un)

        if product_detail.empty and gift_detail.empty:
            st.error("没有匹配到任何数据，请检查库位表和盘点表文件")
            return

        st.subheader("🔍 空库位数据汇总")
        empty_product_df = pd.DataFrame()
        empty_gift_df = pd.DataFrame()
        io_empty = None
        if product_empty:
            df_pe = pd.DataFrame(product_empty)
            total_pe = df_pe.sum().to_frame().T
            total_pe.insert(0, '类别', '成品空库位总计')
            empty_product_df = total_pe
        if gift_empty:
            df_ge = pd.DataFrame(gift_empty)
            total_ge = df_ge.sum().to_frame().T
            total_ge.insert(0, '类别', '赠品空库位总计')
            empty_gift_df = total_ge

        if not empty_product_df.empty or not empty_gift_df.empty:
            sheet_dict = {}
            if not empty_product_df.empty:
                sheet_dict['成品空库位'] = empty_product_df
            if not empty_gift_df.empty:
                sheet_dict['赠品空库位'] = empty_gift_df
            io_empty = safe_write_excel(sheet_dict)
            st.download_button("📥 下载空库位汇总", io_empty, file_name="空库位汇总.xlsx")
            if not empty_product_df.empty:
                st.dataframe(empty_product_df)
            if not empty_gift_df.empty:
                st.dataframe(empty_gift_df)
        else:
            st.info("✅ 未发现空库位数据")

        st.subheader("🧾 表尾合计行汇总（分成品/赠品）")
        io_summary = None
        if summary_rows:
            sum_df = pd.DataFrame(summary_rows)
            
            # ========== 新增：将“文件名”列简化为仓库名（只保留“--”后的部分） ==========
            if not sum_df.empty and '文件名' in sum_df.columns:
                sum_df['文件名'] = sum_df['文件名'].apply(
                    lambda x: x.split('--')[-1].split('.')[0].strip() if '--' in x else x.split('/')[-1].split('.')[0]
                )
            # ========================================================================

            for col in SUM_COLUMNS:
                if col not in sum_df.columns:
                    sum_df[col] = 0.0
            if '盘盈（+）' in sum_df.columns:
                sum_df['盘盈'] = sum_df['盘盈（+）'].apply(lambda x: x if x > 0 else 0)
                sum_df['盘亏'] = sum_df['盘盈（+）'].apply(lambda x: abs(x) if x < 0 else 0)
                sum_df.drop('盘盈（+）', axis=1, inplace=True)
            else:
                sum_df['盘盈'] = 0.0
                sum_df['盘亏'] = 0.0

            product_mask = sum_df['Sheet'].str.contains('成品|正品', na=False)
            gift_mask = sum_df['Sheet'].str.contains('赠品', na=False)
            product_sum_rows = sum_df[product_mask].copy()
            gift_sum_rows = sum_df[gift_mask].copy()

            order_cols = ['文件名', 'Sheet', 'ERP账面数量', '入库未记数', '出库未记数', '调整后数量', '实盘数量', '盘盈', '盘亏']
            product_sum_rows = product_sum_rows[[c for c in order_cols if c in product_sum_rows.columns]]
            gift_sum_rows = gift_sum_rows[[c for c in order_cols if c in gift_sum_rows.columns]]

            if not product_sum_rows.empty or not gift_sum_rows.empty:
                sheet_dict = {}
                if not product_sum_rows.empty:
                    sheet_dict['成品合计行'] = product_sum_rows
                if not gift_sum_rows.empty:
                    sheet_dict['赠品合计行'] = gift_sum_rows
                io_summary = safe_write_excel(sheet_dict)
                st.download_button("📥 下载表尾合计行汇总", io_summary, file_name="表尾合计行汇总.xlsx")
            else:
                st.info("未提取到表尾合计行")

            if not product_sum_rows.empty:
                st.write("**成品合计行**")
                st.dataframe(product_sum_rows)
            if not gift_sum_rows.empty:
                st.write("**赠品合计行**")
                st.dataframe(gift_sum_rows)
        else:
            st.info("未提取到表尾合计行")

        product_summary = summarize_by_warehouse(product_detail.copy())
        gift_summary = summarize_by_warehouse(gift_detail.copy())
        if not product_summary.empty:
            total_p = product_summary.iloc[:, 1:].sum().to_frame().T
            total_p.insert(0, '仓库描述', '总计')
            product_summary = pd.concat([product_summary, total_p], ignore_index=True)
        if not gift_summary.empty:
            total_g = gift_summary.iloc[:, 1:].sum().to_frame().T
            total_g.insert(0, '仓库描述', '总计')
            gift_summary = pd.concat([gift_summary, total_g], ignore_index=True)

        st.subheader("📋 按仓库汇总结果")
        col1, col2 = st.columns(2)
        with col1:
            st.dataframe(product_summary)
        with col2:
            st.dataframe(gift_summary)

        product_output = align_to_fixed_columns(product_detail, FIXED_COLUMNS)
        gift_output = align_to_fixed_columns(gift_detail, FIXED_COLUMNS)
        with st.expander("查看明细"):
            if not product_output.empty:
                st.dataframe(product_output)
            if not gift_output.empty:
                st.dataframe(gift_output)

        if empty_warnings:
            with st.expander("⚠️ 空库位明细警告"):
                st.dataframe(pd.DataFrame(empty_warnings))

        updated_file = None
        if match_file is not None:
            updated_file = update_match_file(match_file.getvalue(), product_summary, gift_summary)
            if updated_file:
                st.success("匹配文件更新成功")
            else:
                st.warning("匹配文件更新失败")

        st.subheader("📦 打包下载所有结果")
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            if not product_summary.empty:
                zf.writestr("成品按仓库汇总.csv", product_summary.to_csv(index=False).encode('utf-8-sig'))
            if not gift_summary.empty:
                zf.writestr("赠品按仓库汇总.csv", gift_summary.to_csv(index=False).encode('utf-8-sig'))
            if not product_output.empty:
                zf.writestr("成品明细.csv", product_output.to_csv(index=False).encode('utf-8-sig'))
            if not gift_output.empty:
                zf.writestr("赠品明细.csv", gift_output.to_csv(index=False).encode('utf-8-sig'))
            if empty_warnings:
                zf.writestr("空库位明细报告.csv", pd.DataFrame(empty_warnings).to_csv(index=False).encode('utf-8-sig'))
            if io_empty is not None:
                zf.writestr("空库位汇总.xlsx", io_empty.getvalue())
            if io_summary is not None:
                zf.writestr("表尾合计行汇总.xlsx", io_summary.getvalue())
            if updated_file:
                original_name = match_file.name if match_file else "匹配文件.xlsx"
                zf.writestr(f"更新_{original_name}", updated_file.getvalue())
        zip_buffer.seek(0)
        st.download_button("📥 下载所有结果 ZIP", zip_buffer, file_name=f"盘点汇总_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip")

        if updated_file and match_file:
            if st.button("➕ 将更新后的匹配文件加入缓存"):
                add_to_cache(updated_file.getvalue(), match_file.name)

    st.success("处理完成！")

if process_btn or re_process_btn:
    run_processing(location_file, inventory_zip, match_file)
