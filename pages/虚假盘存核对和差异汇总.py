import streamlit as st
import pandas as pd
import numpy as np
import re
from datetime import datetime
import io

st.set_page_config(page_title="库存差异核对", layout="wide")
st.title("📊 WMS与ERP库存差异核对")

# 侧边栏说明
with st.sidebar:
    st.markdown("### 使用说明")
    st.markdown("1. 上传 **WMS与R3库存差异报表**（Excel）")
    st.markdown("2. 上传 **美菱盘点明细汇总**（Excel，需包含“成品”“赠品”工作表）")
    st.markdown("3. 上传 **RDC仓库编号表**（可选，包含“仓库编号”“RDC名称”列）")
    st.markdown("4. 点击「开始核对」")
    st.markdown("5. 下载生成的结果文件")

# 文件上传
diff_file = st.file_uploader("📁 WMS与R3库存差异报表", type=["xlsx", "xls"])
pan_file = st.file_uploader("📁 美菱盘点明细汇总", type=["xlsx", "xls"])
rdc_file = st.file_uploader("📁 RDC仓库编号表（可选）", type=["xlsx", "xls"])

# 定义所有处理函数（与之前完全一致，无需改动）
def find_column(df, patterns):
    for col in df.columns:
        for pat in patterns:
            if re.search(pat, col, re.IGNORECASE):
                return col
    return None

def read_pan_from_upload(file_bytes):
    """从上传的 Excel 字节流读取成品和赠品工作表"""
    sheets = ['成品', '赠品']
    pan_dfs = {}
    for sheet in sheets:
        try:
            df = pd.read_excel(file_bytes, sheet_name=sheet)
            pan_col = find_column(df, ['盘盈', '盘亏'])
            if pan_col is None:
                st.warning(f"工作表 [{sheet}] 未找到盘盈/盘亏列，跳过")
                continue
            needed_match = ['物料代码', '工厂', '库位', '产品等级']
            missing = [c for c in needed_match if c not in df.columns]
            if missing:
                st.warning(f"工作表 [{sheet}] 缺少匹配列：{missing}，跳过")
                continue
            for col in ['入库未记数', '出库未记数']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                else:
                    df[col] = 0
            df.attrs['盘盈盘亏原列名'] = pan_col
            df['_盘盈盘亏数量'] = pd.to_numeric(df[pan_col], errors='coerce').fillna(0)
            pan_dfs[sheet] = df
        except Exception as e:
            st.warning(f"读取工作表 [{sheet}] 失败：{e}")
    if not pan_dfs:
        raise ValueError("未成功读取任何盘点数据")
    return pan_dfs

def check_condition1(diff_val, out_not_record, in_not_record):
    if pd.isna(diff_val):
        return False, "差异库存为空（未匹配到差异表）"
    if diff_val < 0:
        if np.isclose(diff_val + out_not_record, 0):
            return True, ""
        else:
            return False, f"负数差异 {diff_val} 与出库未记数 {out_not_record} 相加不为0"
    elif diff_val > 0:
        if np.isclose(diff_val - in_not_record, 0):
            return True, ""
        else:
            return False, f"正数差异 {diff_val} 与入库未记数 {in_not_record} 相减不为0"
    else:
        return True, "差异为0"

def check_condition2(out_not_record, in_not_record, pan_diff_qty, unmatched_list, reason):
    unmatched_list = str(unmatched_list) if unmatched_list is not None else ''
    reason = str(reason) if reason is not None else ''
    unmatched_list = unmatched_list.strip()
    reason = reason.strip()
    if unmatched_list.lower() in ['nan', '']:
        unmatched_list = ''
    out_not_record = float(out_not_record) if out_not_record is not None else 0.0
    in_not_record = float(in_not_record) if in_not_record is not None else 0.0
    pan_diff_qty = float(pan_diff_qty) if pan_diff_qty is not None else 0.0

    if out_not_record == 0 and in_not_record == 0 and pan_diff_qty == 0:
        return True, "无异常，跳过单号检查"
    if not unmatched_list:
        return False, "未匹配单号列表为空"

    tokens = re.split(r'[ ,\n\t/、]+', unmatched_list)
    tokens = [t.strip() for t in tokens if t.strip()]
    if not tokens:
        return False, "未匹配单号列表解析为空"

    missing = []
    for token in tokens:
        if token not in reason:
            missing.append(token)
    if missing:
        return False, f"单号 {', '.join(missing)} 未在差异原因分析中找到"
    else:
        return True, ""

def merge_and_mark(diff_df, pan_df, pan_name):
    diff_agg = diff_df.groupby(['货品编号', '工厂编码', '库位编码', '等级'], as_index=False).agg({
        'WMS和ERP的差异库存': 'sum',
        '未匹配单号列表': lambda x: ','.join(x.dropna().astype(str)),
        '仓库描述': lambda x: x.iloc[0] if len(x) > 0 else ''
    })
    pan_match = pan_df.rename(columns={
        '物料代码': '货品编号',
        '工厂': '工厂编码',
        '库位': '库位编码',
        '产品等级': '等级'
    })
    merged = pan_match.merge(diff_agg, on=['货品编号', '工厂编码', '库位编码', '等级'], how='left')
    results = []
    fail_reasons = []
    for idx, row in merged.iterrows():
        out_val = row.get('出库未记数', 0)
        in_val = row.get('入库未记数', 0)
        pan_diff_qty = row.get('_盘盈盘亏数量', 0)
        if out_val == 0 and in_val == 0 and pan_diff_qty == 0:
            results.append("否")
            fail_reasons.append("")
            continue
        diff_val = row.get('WMS和ERP的差异库存', np.nan)
        unmatched = row.get('未匹配单号列表', '')
        reason = row.get('差异原因分析', '')
        if not isinstance(reason, str):
            reason = str(reason) if reason is not None else ''
        c1_ok, c1_msg = check_condition1(diff_val, out_val, in_val)
        c2_ok, c2_msg = check_condition2(out_val, in_val, pan_diff_qty, unmatched, reason)
        if c1_ok and c2_ok:
            results.append("否")
            fail_reasons.append("")
        else:
            results.append("需要核实")
            reasons = []
            if not c1_ok:
                reasons.append(f"条件1: {c1_msg}")
            if not c2_ok:
                reasons.append(f"条件2: {c2_msg}")
            fail_reasons.append("；".join(reasons))
    pan_marked = pan_df.copy()
    pan_marked['是否虚假盘存'] = results
    pan_marked['失败原因'] = fail_reasons
    if '_盘盈盘亏数量' in pan_marked.columns:
        pan_marked.drop(columns=['_盘盈盘亏数量'], inplace=True)
    return pan_marked

def get_diff_missing_records(diff_df, pan_dfs):
    all_pan_keys = set()
    for pan_name, pan_df in pan_dfs.items():
        pan_keys = pan_df[['物料代码', '工厂', '库位', '产品等级']].drop_duplicates()
        for _, row in pan_keys.iterrows():
            key = (row['物料代码'], row['工厂'], row['库位'], row['产品等级'])
            all_pan_keys.add(key)
    diff_keys = diff_df[['货品编号', '工厂编码', '库位编码', '等级']].copy()
    diff_keys['key'] = list(zip(diff_keys['货品编号'], diff_keys['工厂编码'], diff_keys['库位编码'], diff_keys['等级']))
    mask = ~diff_keys['key'].isin(all_pan_keys)
    missing = diff_df[mask].copy()
    missing['是否虚假盘存'] = '需要核实'
    missing['失败原因'] = '盘点表无对应记录'
    return missing

def create_warehouse_summary(pan_dfs, diff_df):
    all_pan_rows = []
    for pan_name, pan_df in pan_dfs.items():
        if '仓库描述' not in pan_df.columns:
            pan_match = pan_df.rename(columns={
                '物料代码': '货品编号',
                '工厂': '工厂编码',
                '库位': '库位编码',
                '产品等级': '等级'
            })
            diff_desc = diff_df[['货品编号', '工厂编码', '库位编码', '等级', '仓库描述']].drop_duplicates()
            pan_with_desc = pan_match.merge(diff_desc, on=['货品编号', '工厂编码', '库位编码', '等级'], how='left')
            pan_with_desc['仓库描述'] = pan_with_desc['仓库描述'].fillna('未知仓库')
            temp_df = pan_with_desc[['仓库描述', '出库未记数', '入库未记数']]
        else:
            temp_df = pan_df[['仓库描述', '出库未记数', '入库未记数']].copy()
            temp_df['仓库描述'] = temp_df['仓库描述'].fillna('未知仓库')
        all_pan_rows.append(temp_df)
    pan_all = pd.concat(all_pan_rows, ignore_index=True)
    pan_summary = pan_all.groupby('仓库描述').agg({
        '出库未记数': 'sum',
        '入库未记数': 'sum'
    }).reset_index()

    if '仓库描述' not in diff_df.columns:
        diff_df['仓库描述'] = diff_df['仓库编码'].astype(str)
    diff_summary = diff_df.groupby('仓库描述', group_keys=False).apply(
        lambda g: pd.Series({
            '出库下账异常数量': g[g['WMS和ERP的差异库存'] < 0]['WMS和ERP的差异库存'].abs().sum(),
            '入库下账异常数量': g[g['WMS和ERP的差异库存'] > 0]['WMS和ERP的差异库存'].sum()
        }), include_groups=False
    ).reset_index()

    merged = pd.merge(diff_summary, pan_summary, on='仓库描述', how='outer').fillna(0)
    merged['差异_出库'] = merged['出库下账异常数量'] - merged['出库未记数']
    merged['差异_入库'] = merged['入库下账异常数量'] - merged['入库未记数']
    merged['有差异地区'] = np.where((merged['差异_出库'] != 0) | (merged['差异_入库'] != 0), '是', '否')
    mask = (merged['出库下账异常数量'] != 0) | (merged['出库未记数'] != 0) | (merged['入库下账异常数量'] != 0) | (merged['入库未记数'] != 0)
    result = merged[mask].copy()
    if result.empty:
        return result
    result.columns = ['仓库描述', '出库下账异常数量-WMS差异表', '入库下账异常数量-WMS差异表',
                      '出库未记数-盘点明细表', '入库未记数-盘点明细表', '差异-出库', '差异-入库', '有差异地区']
    result = result[['仓库描述', '出库下账异常数量-WMS差异表', '出库未记数-盘点明细表', '差异-出库',
                     '入库下账异常数量-WMS差异表', '入库未记数-盘点明细表', '差异-入库', '有差异地区']]
    return result

def create_cross_summary(diff_df):
    if '差异类型' not in diff_df.columns:
        return None
    cross = diff_df[['仓库描述', '差异类型', 'WMS和ERP的差异库存']].dropna(subset=['仓库描述', '差异类型'])
    cross['abs_diff'] = cross['WMS和ERP的差异库存'].abs()
    pivot = cross.pivot_table(index='仓库描述', columns='差异类型', values='abs_diff', aggfunc='sum', fill_value=0)
    if pivot.empty:
        return pivot
    total_row = pivot.sum(axis=0)
    pivot.loc['总计'] = total_row
    pivot = pivot.reset_index()
    return pivot

# 主逻辑
if diff_file and pan_file:
    if st.button("🚀 开始核对", type="primary"):
        with st.spinner("正在处理，请稍候..."):
            try:
                # 读取差异表
                diff_df = pd.read_excel(diff_file)
                required_diff = ['货品编号', '工厂编码', '库位编码', '等级', 'WMS和ERP的差异库存', '未匹配单号列表', '仓库编码']
                missing = [c for c in required_diff if c not in diff_df.columns]
                if missing:
                    st.error(f"差异表缺少列：{missing}")
                    st.stop()

                # 处理RDC映射
                rdc_map = {}
                if rdc_file:
                    rdc_df = pd.read_excel(rdc_file)
                    if '仓库编号' in rdc_df.columns and 'RDC名称' in rdc_df.columns:
                        rdc_map = dict(zip(rdc_df['仓库编号'].astype(str).str.strip(),
                                           rdc_df['RDC名称'].astype(str).str.strip()))
                if rdc_map:
                    diff_df['仓库描述'] = diff_df['仓库编码'].astype(str).str.strip().map(rdc_map)
                    diff_df['仓库描述'] = diff_df['仓库描述'].fillna(diff_df['仓库编码'].astype(str))
                else:
                    diff_df['仓库描述'] = diff_df['仓库编码'].astype(str)

                diff_df['WMS和ERP的差异库存'] = pd.to_numeric(diff_df['WMS和ERP的差异库存'], errors='coerce')
                diff_df['未匹配单号列表'] = diff_df['未匹配单号列表'].fillna('').astype(str)

                # 读取盘点明细
                pan_dfs = read_pan_from_upload(pan_file)

                # 处理每个盘点表
                marked_pan_dfs = {}
                all_pan_failed = []
                for pan_name, pan_df in pan_dfs.items():
                    marked = merge_and_mark(diff_df, pan_df, pan_name)
                    marked_pan_dfs[pan_name] = marked
                    failed = marked[marked['是否虚假盘存'] == '需要核实'].copy()
                    if not failed.empty:
                        pan_col_name = pan_df.attrs.get('盘盈盘亏原列名', '盘盈盘亏数量')
                        key_cols = ['物料代码', '工厂', '库位', '产品等级', '出库未记数', '入库未记数', pan_col_name, '差异原因分析']
                        exist_cols = [c for c in key_cols if c in failed.columns]
                        extra_cols = ['WMS和ERP的差异库存', '未匹配单号列表']
                        exist_extra = [c for c in extra_cols if c in failed.columns]
                        final_cols = exist_cols + exist_extra + ['是否虚假盘存', '失败原因']
                        failed_sub = failed[final_cols].copy()
                        failed_sub['数据来源'] = f'盘点表_{pan_name}'
                        all_pan_failed.append(failed_sub)

                # 差异表无匹配记录
                missing_diff = get_diff_missing_records(diff_df, pan_dfs)
                diff_failed = pd.DataFrame()
                if not missing_diff.empty:
                    key_diff_cols = ['货品编号', '工厂编码', '库位编码', '等级', '仓库描述', 'WMS和ERP的差异库存', '未匹配单号列表', '失败原因']
                    exist_diff = [c for c in key_diff_cols if c in missing_diff.columns]
                    diff_failed = missing_diff[exist_diff].copy()
                    diff_failed['是否虚假盘存'] = '需要核实'
                    diff_failed['数据来源'] = '差异表'

                # 为差异表添加标记
                success_keys = set()
                for pan_name, marked_df in marked_pan_dfs.items():
                    success = marked_df[marked_df['是否虚假盘存'] == '否']
                    for _, row in success.iterrows():
                        key = (row['物料代码'], row['工厂'], row['库位'], row['产品等级'])
                        success_keys.add(key)
                diff_df['是否虚假盘存'] = diff_df.apply(
                    lambda row: '否' if (row['货品编号'], row['工厂编码'], row['库位编码'], row['等级']) in success_keys else '需要核实',
                    axis=1
                )

                # 生成汇总表
                summary1 = create_warehouse_summary(pan_dfs, diff_df)
                summary2 = create_cross_summary(diff_df)

                # 将结果写入内存中的 Excel 文件
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    diff_df.to_excel(writer, sheet_name='差异表_标记', index=False)
                    for pan_name, marked_df in marked_pan_dfs.items():
                        marked_df.to_excel(writer, sheet_name=f'盘点表_{pan_name}_标记', index=False)
                    if not summary1.empty:
                        summary1.to_excel(writer, sheet_name='仓库汇总', index=False)
                    if summary2 is not None and not summary2.empty:
                        summary2.to_excel(writer, sheet_name='差异类型汇总', index=False)
                    if all_pan_failed:
                        pan_failed_all = pd.concat(all_pan_failed, ignore_index=True)
                        pan_failed_all.to_excel(writer, sheet_name='盘点表失败明细', index=False)
                    if not diff_failed.empty:
                        diff_failed.to_excel(writer, sheet_name='差异表失败明细', index=False)
                output.seek(0)

                # 提供下载按钮
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f'inventory_check_result_{timestamp}.xlsx'
                st.success("✅ 处理完成！点击下方按钮下载结果文件。")
                st.download_button(
                    label="📥 下载结果 Excel",
                    data=output,
                    file_name=filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            except Exception as e:
                st.error(f"处理过程中出现错误：{str(e)}")
else:
    st.info("请上传所有必需文件后点击「开始核对」")
