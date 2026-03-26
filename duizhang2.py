# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import io
import gc
import zipfile
import tempfile
import os
from datetime import datetime

# ========== 页面配置 ==========
st.set_page_config(
    page_title="库存对账工具",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ========== 初始化会话状态 ==========
if 'last_reconciliation_result' not in st.session_state:
    st.session_state['last_reconciliation_result'] = None
if 'last_summary' not in st.session_state:
    st.session_state['last_summary'] = None
if 'last_wms_marked' not in st.session_state:
    st.session_state['last_wms_marked'] = None

# ========== 自定义CSS ==========
st.markdown("""
<style>
    * {
        font-family: "Microsoft YaHei", "SimHei", "PingFang SC", "Helvetica Neue", Roboto, sans-serif;
    }
    .stButton button {
        font-size: 16px;
        font-weight: 500;
    }
</style>
""", unsafe_allow_html=True)

# ========== 列名配置 ==========
COL_ORDER_WMS = 'LRP单号'
COL_COMMON_NO = '单号'
COL_MATERIAL_WMS = '货品编码'
COL_PLANT_WMS = '工厂'
COL_STORAGE_WMS = 'ERP库位'
COL_QTY_WMS = '数量'
COL_INOUT = '进or出'
COL_KEEPER = '保管员'
COL_TRANS_TYPE = '交易类型'

COL_ORDER_R3 = '前继单号'
COL_QTY_R3 = '数量'

COL_ORDER_SALES = '运单号'
COL_MSG_SALES = '返回消息'

COL_MATERIAL_TARGET = '货品编号'
COL_PLANT_TARGET = '工厂编码'
COL_STORAGE_TARGET = '库位编码'
COL_DIFF_TARGET = 'WMS和ERP的差异库存'
COL_WAREHOUSE_TARGET = '仓库编码'

COL_RDC_WAREHOUSE = '仓库编号'

KEYWORD_DIFF_TYPE = {
    '超账期冻结': '客户代码冻结',
    '定价错误': '价格未维护',
    '出具发票方与付款方必须一致': '门店代码未维护'
}

# ========== 辅助函数 ==========
def clean_str(val):
    if pd.isna(val):
        return ''
    s = str(val).strip()
    try:
        if '.' in s:
            f = float(s)
            if f.is_integer():
                s = str(int(f))
    except:
        pass
    return s

def clean_float(val):
    try:
        return float(val)
    except:
        return 0.0

def get_diff_type(msg):
    if pd.isna(msg) or msg == '':
        return ''
    types = []
    for keyword, diff_type in KEYWORD_DIFF_TYPE.items():
        if keyword in msg:
            types.append(diff_type)
    types = list(dict.fromkeys(types))
    return ';'.join(types)

def read_file(file):
    if file is None:
        return None
    try:
        if file.name.endswith('.csv'):
            return pd.read_csv(file, encoding='utf-8-sig')
        elif file.name.endswith('.xls'):
            return pd.read_excel(file, engine='xlrd')
        else:
            return pd.read_excel(file, engine='openpyxl')
    except Exception:
        try:
            return pd.read_excel(file)
        except:
            file.seek(0)
            return pd.read_csv(file, encoding='utf-8-sig')

@st.cache_data
def get_r3_sets(df_r3):
    outbound = set(df_r3[df_r3[COL_QTY_R3] < 0][COL_ORDER_R3].astype(str))
    inbound = set(df_r3[df_r3[COL_QTY_R3] > 0][COL_ORDER_R3].astype(str))
    return outbound, inbound

def process_data(df_wms, df_r3, df_sales, df_target, df_rdc, skip_rdc_match):
    """核心对账处理逻辑（内存优化版）"""
    r3_outbound, r3_inbound = get_r3_sets(df_r3)

    df_wms[COL_ORDER_WMS] = df_wms[COL_ORDER_WMS].astype(str).apply(clean_str)
    df_wms[COL_COMMON_NO] = df_wms[COL_COMMON_NO].astype(str).apply(clean_str)

    for col in [COL_MATERIAL_WMS, COL_PLANT_WMS, COL_STORAGE_WMS, COL_INOUT, COL_KEEPER, COL_TRANS_TYPE]:
        if col in df_wms.columns:
            df_wms[col] = df_wms[col].astype(str).apply(clean_str)

    df_wms[COL_QTY_WMS] = df_wms[COL_QTY_WMS].apply(clean_float)

    cond1 = (df_wms[COL_INOUT] == 'OUT') & (df_wms[COL_TRANS_TYPE] == '出库')
    cond2 = (df_wms[COL_INOUT] == 'OUT') & (df_wms[COL_TRANS_TYPE] == '取消出库')
    cond3 = (df_wms[COL_INOUT] == 'IN') & (df_wms[COL_TRANS_TYPE] == '收货')
    df_wms = df_wms[cond1 | cond2 | cond3]
    df_wms = df_wms[df_wms[COL_KEEPER] != '系统API']

    if df_wms.empty:
        return None, None, None

    out_records = df_wms[(df_wms[COL_INOUT] == 'OUT') & (df_wms[COL_TRANS_TYPE] == '出库')].copy()
    cancel_records = df_wms[(df_wms[COL_INOUT] == 'OUT') & (df_wms[COL_TRANS_TYPE] == '取消出库')].copy()
    receive_records = df_wms[(df_wms[COL_INOUT] == 'IN') & (df_wms[COL_TRANS_TYPE] == '收货')].copy()

    if not out_records.empty:
        out_records['匹配'] = out_records[COL_ORDER_WMS].apply(lambda x: '是' if x in r3_outbound else '否')
        unmatched_out = out_records[out_records['匹配'] == '否']
    else:
        unmatched_out = pd.DataFrame()

    if not receive_records.empty:
        receive_records['匹配'] = receive_records[COL_ORDER_WMS].apply(lambda x: '是' if x in r3_inbound else '否')
        unmatched_receive = receive_records[receive_records['匹配'] == '否']
    else:
        unmatched_receive = pd.DataFrame()

    if not cancel_records.empty:
        cancel_records['匹配'] = '否'
        unmatched_cancel = cancel_records.copy()
    else:
        unmatched_cancel = pd.DataFrame()

    # 正负抵消
    if not unmatched_out.empty:
        out_agg = unmatched_out.groupby([COL_ORDER_WMS, COL_MATERIAL_WMS, COL_PLANT_WMS, COL_STORAGE_WMS]).agg(
            out_qty=(COL_QTY_WMS, 'sum'),
            common_no=(COL_COMMON_NO, 'first')
        ).reset_index()
        out_agg.rename(columns={COL_ORDER_WMS: 'lrp_order'}, inplace=True)
    else:
        out_agg = pd.DataFrame()

    if not unmatched_cancel.empty:
        cancel_agg = unmatched_cancel.groupby(COL_COMMON_NO).agg(
            cancel_qty=(COL_QTY_WMS, 'sum')
        ).reset_index()
    else:
        cancel_agg = pd.DataFrame()

    net_records = pd.DataFrame()
    if not out_agg.empty and not cancel_agg.empty:
        combined = pd.merge(out_agg, cancel_agg, left_on='common_no', right_on=COL_COMMON_NO, how='outer').fillna(0)
        combined['净数量'] = combined['out_qty'] - combined['cancel_qty']
        combined = combined[combined['净数量'] > 0]

        if not combined.empty:
            net_records = combined[['lrp_order', '净数量', COL_MATERIAL_WMS, COL_PLANT_WMS, COL_STORAGE_WMS]].copy()
            net_records.rename(columns={
                '净数量': COL_QTY_WMS,
                'lrp_order': COL_ORDER_WMS
            }, inplace=True)
            net_records['记录类型'] = '出库'
        del combined
        gc.collect()

    if not unmatched_receive.empty:
        unmatched_receive['记录类型'] = '收货'

    all_unmatched = pd.concat([net_records, unmatched_receive], ignore_index=True)

    if all_unmatched.empty:
        df_summary = pd.DataFrame(columns=[COL_MATERIAL_WMS, COL_PLANT_WMS, COL_STORAGE_WMS,
                                           '未匹配单号列表', '未匹配单号个数', '数量', '返回消息', '差异类型',
                                           '出库数量', '收货数量'])
        df_wms_marked = pd.DataFrame()
    else:
        group_cols = [COL_MATERIAL_WMS, COL_PLANT_WMS, COL_STORAGE_WMS, COL_ORDER_WMS]
        lrp_summary_list = []

        if not net_records.empty:
            out_by_lrp = net_records.groupby(group_cols).agg(出库数量=(COL_QTY_WMS, 'sum')).reset_index()
            out_by_lrp['收货数量'] = 0
            lrp_summary_list.append(out_by_lrp)

        if not unmatched_receive.empty:
            in_by_lrp = unmatched_receive.groupby(group_cols).agg(收货数量=(COL_QTY_WMS, 'sum')).reset_index()
            in_by_lrp['出库数量'] = 0
            lrp_summary_list.append(in_by_lrp)

        if lrp_summary_list:
            lrp_summary = pd.concat(lrp_summary_list, ignore_index=True)
            lrp_summary = lrp_summary.groupby(group_cols).agg(
                出库数量=('出库数量', 'sum'),
                收货数量=('收货数量', 'sum')
            ).reset_index()
        else:
            lrp_summary = pd.DataFrame()

        if not lrp_summary.empty and df_sales is not None and not df_sales.empty:
            if COL_ORDER_SALES in df_sales.columns and COL_MSG_SALES in df_sales.columns:
                df_sales[COL_ORDER_SALES] = df_sales[COL_ORDER_SALES].astype(str).apply(clean_str)
                df_sales[COL_MSG_SALES] = df_sales[COL_MSG_SALES].astype(str).apply(clean_str)
                df_sales_unique = df_sales.drop_duplicates(subset=[COL_ORDER_SALES], keep='first')
                lrp_summary = lrp_summary.merge(
                    df_sales_unique[[COL_ORDER_SALES, COL_MSG_SALES]],
                    left_on=COL_ORDER_WMS,
                    right_on=COL_ORDER_SALES,
                    how='left'
                )
                lrp_summary[COL_MSG_SALES] = lrp_summary[COL_MSG_SALES].fillna('')
                lrp_summary.rename(columns={COL_MSG_SALES: '返回消息'}, inplace=True)
            else:
                lrp_summary['返回消息'] = ''
        elif not lrp_summary.empty:
            lrp_summary['返回消息'] = ''

        if not lrp_summary.empty:
            lrp_summary['差异类型'] = lrp_summary['返回消息'].apply(get_diff_type)

        summary_list = []
        if not lrp_summary.empty:
            for (material, plant, storage), group in lrp_summary.groupby([COL_MATERIAL_WMS, COL_PLANT_WMS, COL_STORAGE_WMS]):
                out_qty = group['出库数量'].sum()
                in_qty = group['收货数量'].sum()
                order_list = '\n'.join(group[COL_ORDER_WMS].astype(str))
                order_count = len(group)
                msg_list = '\n'.join(group['返回消息'].astype(str))
                diff_types = ';'.join(sorted(set([v for v in group['差异类型'] if v != ''])))

                summary_list.append({
                    COL_MATERIAL_WMS: material,
                    COL_PLANT_WMS: plant,
                    COL_STORAGE_WMS: storage,
                    '未匹配单号列表': order_list,
                    '未匹配单号个数': order_count,
                    '出库数量': out_qty,
                    '收货数量': in_qty,
                    '返回消息': msg_list,
                    '差异类型': diff_types
                })

        df_summary = pd.DataFrame(summary_list) if summary_list else pd.DataFrame()

        if not df_summary.empty:
            df_summary['数量'] = df_summary['出库数量'].astype(int)

        all_matched = pd.concat([
            out_records[out_records['匹配'] == '是'] if not out_records.empty else pd.DataFrame(),
            receive_records[receive_records['匹配'] == '是'] if not receive_records.empty else pd.DataFrame()
        ], ignore_index=True)
        all_unmatched_temp = pd.concat([unmatched_out, unmatched_cancel, unmatched_receive], ignore_index=True)
        df_wms_marked = pd.concat([all_matched, all_unmatched_temp], ignore_index=True)

    del out_records, cancel_records, receive_records
    del unmatched_out, unmatched_cancel, unmatched_receive
    del out_agg, cancel_agg, net_records
    gc.collect()

    if df_target is not None and not df_target.empty:
        key_cols_target = [COL_MATERIAL_TARGET, COL_PLANT_TARGET, COL_STORAGE_TARGET, COL_DIFF_TARGET, COL_WAREHOUSE_TARGET]
        for col in key_cols_target:
            if col in df_target.columns:
                df_target[col] = df_target[col].astype(str).apply(clean_str)

        if COL_DIFF_TARGET in df_target.columns:
            df_target[COL_DIFF_TARGET] = pd.to_numeric(df_target[COL_DIFF_TARGET], errors='coerce').fillna(0)
            df_target = df_target[df_target[COL_DIFF_TARGET] != 0]

        if not skip_rdc_match and df_rdc is not None and not df_rdc.empty:
            if COL_RDC_WAREHOUSE in df_rdc.columns and COL_WAREHOUSE_TARGET in df_target.columns:
                rdc_wh_list = set(df_rdc[COL_RDC_WAREHOUSE].astype(str).apply(clean_str))
                df_target = df_target[df_target[COL_WAREHOUSE_TARGET].isin(rdc_wh_list)]

        if not df_summary.empty:
            df_target['未匹配单号列表'] = ''
            df_target['未匹配单号个数'] = 0
            df_target['数量'] = 0
            df_target['返回消息'] = ''
            df_target['差异类型'] = ''
            df_target['调整后差异'] = 0.0

            for col in [COL_MATERIAL_WMS, COL_PLANT_WMS, COL_STORAGE_WMS]:
                if col in df_summary.columns:
                    df_summary[col] = df_summary[col].astype(str).apply(clean_str)

            summary_dict = {}
            for _, row in df_summary.iterrows():
                key = (row[COL_MATERIAL_WMS], row[COL_PLANT_WMS], row[COL_STORAGE_WMS])
                summary_dict[key] = row.to_dict()

            for idx, row in df_target.iterrows():
                key = (row[COL_MATERIAL_TARGET], row[COL_PLANT_TARGET], row[COL_STORAGE_TARGET])
                if key in summary_dict:
                    rec = summary_dict[key]
                    df_target.at[idx, '未匹配单号列表'] = str(rec.get('未匹配单号列表', ''))
                    df_target.at[idx, '未匹配单号个数'] = int(rec.get('未匹配单号个数', 0))
                    df_target.at[idx, '数量'] = int(rec.get('数量', 0))
                    df_target.at[idx, '返回消息'] = str(rec.get('返回消息', ''))
                    df_target.at[idx, '差异类型'] = str(rec.get('差异类型', ''))

                    out_qty = rec.get('出库数量', 0)
                    in_qty = rec.get('收货数量', 0)
                    total_unmatched_qty = out_qty + in_qty
                    diff_value = row[COL_DIFF_TARGET]

                    if diff_value > 0:
                        adjusted_diff = diff_value - total_unmatched_qty
                    elif diff_value < 0:
                        adjusted_diff = diff_value + total_unmatched_qty
                    else:
                        adjusted_diff = diff_value
                    df_target.at[idx, '调整后差异'] = adjusted_diff
                else:
                    df_target.at[idx, '调整后差异'] = row[COL_DIFF_TARGET]

        return df_wms_marked, df_summary, df_target

    return df_wms_marked, df_summary, None


# ========== 功能1：库存查询 ==========
def inventory_query():
    st.header("🔍 库存查询")
    st.write("这是一个示例功能，您可以在此添加自定义的库存查询逻辑。")
    st.info("例如：按物料编码查询库存分布，或连接数据库实时查询。")

    material_code = st.text_input("请输入物料编码")
    if material_code:
        st.write(f"您查询的物料编码是：**{material_code}**")
        st.warning("目前仅展示示例，实际功能可自行开发。")


# ========== 功能2：核对盘存问题 ==========
def check_inventory_problems():
    st.header("🔍 核对盘存问题")
    st.write("基于上次对账结果，分析盘点差异是否存在异常。")

    if st.session_state['last_reconciliation_result'] is None:
        st.warning("⚠️ 请先执行对账功能，生成差异报表后再使用此功能。")
        st.info("请前往「库存对账工具」页面上传文件并完成对账。")
        return

    df = st.session_state['last_reconciliation_result']
    st.subheader("📊 当前差异报表数据概览")
    st.dataframe(df.head(10), use_container_width=True)
    st.caption(f"共 {len(df)} 行数据")

    st.subheader("📈 差异分析")
    diff_col = 'WMS和ERP的差异库存'
    if diff_col in df.columns:
        df['差异绝对值'] = df[diff_col].abs()
        threshold = st.number_input("设置差异绝对值阈值", min_value=0, value=5, step=1)
        problematic = df[df['差异绝对值'] > threshold]
        if problematic.empty:
            st.success(f"✅ 所有行差异绝对值均 ≤ {threshold}，盘点差异在可接受范围内。")
        else:
            st.warning(f"⚠️ 共有 {len(problematic)} 行差异绝对值超过 {threshold}，可能存在盘点问题：")
            st.dataframe(problematic, use_container_width=True)
    else:
        st.error(f"列 '{diff_col}' 不存在，请检查目标报表列名是否正确。")


# ========== 功能3：汇总所有盘点表 ==========
def summarize_inventory_sheets():
    st.header("📚 汇总所有盘点表")
    st.write("上传多个盘点表文件（Excel/CSV）或一个压缩包（ZIP），系统将自动合并汇总。")

    uploaded_files = st.file_uploader(
        "选择盘点表文件（可多选，也支持 ZIP 压缩包）",
        type=['xlsx', 'xls', 'csv', 'zip'],
        accept_multiple_files=True,
        key="inventory_sheets"
    )

    if uploaded_files:
        all_data = []
        for file in uploaded_files:
            if file.name.endswith('.zip'):
                with tempfile.TemporaryDirectory() as tmpdir:
                    zip_path = os.path.join(tmpdir, file.name)
                    with open(zip_path, 'wb') as f:
                        f.write(file.getbuffer())
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.extractall(tmpdir)
                    for root, dirs, files in os.walk(tmpdir):
                        for filename in files:
                            if filename.endswith(('.xlsx', '.xls', '.csv')):
                                file_path = os.path.join(root, filename)
                                try:
                                    if filename.endswith('.csv'):
                                        df = pd.read_csv(file_path, encoding='utf-8-sig')
                                    else:
                                        df = pd.read_excel(file_path, engine='openpyxl')
                                    df['来源文件'] = filename
                                    all_data.append(df)
                                    st.success(f"✅ 已从压缩包读取 {filename}")
                                except Exception as e:
                                    st.error(f"❌ 读取压缩包内文件 {filename} 失败：{e}")
            else:
                try:
                    df = read_file(file)
                    df['来源文件'] = file.name
                    all_data.append(df)
                    st.success(f"✅ 已读取 {file.name}，共 {len(df)} 行")
                except Exception as e:
                    st.error(f"❌ 读取 {file.name} 失败：{e}")

        if all_data:
            combined = pd.concat(all_data, ignore_index=True)
            st.subheader("📊 合并后的汇总表")
            st.dataframe(combined.head(20), use_container_width=True)
            st.caption(f"共 {len(combined)} 行")

            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                combined.to_excel(writer, sheet_name='盘点汇总', index=False)
            st.download_button(
                label="📥 下载汇总表",
                data=buffer.getvalue(),
                file_name=f"盘点汇总_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )


# ========== 功能4：盘点表基础数据制作 ==========
def create_inventory_sheet():
    st.header("📝 盘点表基础数据制作")
    st.write("上传库存基础数据文件，系统将生成带有空白盘点列的盘点表模板，便于线下盘点。")

    uploaded_file = st.file_uploader(
        "选择库存基础数据文件（Excel/CSV）",
        type=['xlsx', 'xls', 'csv'],
        key="inventory_data"
    )

    if uploaded_file is not None:
        try:
            df = read_file(uploaded_file)
            st.success(f"✅ 成功读取文件，共 {len(df)} 行")
            st.dataframe(df.head(10), use_container_width=True)

            st.info("请确认文件中包含以下关键列：物料编码、货品名称、库存数量、库位（可选）")
            
            columns = df.columns.tolist()
            material_col = st.selectbox("请选择物料编码列", columns, key="material_col")
            name_col = st.selectbox("请选择货品名称列", columns, key="name_col")
            qty_col = st.selectbox("请选择库存数量列", columns, key="qty_col")
            location_col = st.selectbox("请选择库位列（可选，若无则选无）", ["无"] + columns, key="location_col")

            group_by = st.radio(
                "分组方式（用于生成多张盘点表）",
                ["不分组", "按库位分组", "按物料分组"]
            )

            if st.button("生成盘点表模板", type="primary"):
                if group_by == "不分组":
                    sheet_data = df[[material_col, name_col, qty_col]].copy()
                    sheet_data.rename(columns={
                        material_col: "物料编码",
                        name_col: "货品名称",
                        qty_col: "账面库存"
                    }, inplace=True)
                    sheet_data["盘点数量"] = ""
                    sheet_data["备注"] = ""
                    
                    buffer = io.BytesIO()
                    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                        sheet_data.to_excel(writer, sheet_name="盘点表", index=False)
                    st.download_button(
                        label="📥 下载盘点表",
                        data=buffer.getvalue(),
                        file_name="盘点表_模板.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                    st.success("盘点表模板生成成功！")

                elif group_by == "按库位分组":
                    if location_col == "无":
                        st.error("未选择库位列，无法按库位分组。")
                        return
                    grouped = df.groupby(location_col)
                    buffer = io.BytesIO()
                    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                        for loc, group in grouped:
                            sheet_name = str(loc)[:31]
                            sheet_data = group[[material_col, name_col, qty_col]].copy()
                            sheet_data.rename(columns={
                                material_col: "物料编码",
                                name_col: "货品名称",
                                qty_col: "账面库存"
                            }, inplace=True)
                            sheet_data["盘点数量"] = ""
                            sheet_data["备注"] = ""
                            sheet_data.to_excel(writer, sheet_name=sheet_name, index=False)
                    st.download_button(
                        label="📥 下载盘点表（按库位分页）",
                        data=buffer.getvalue(),
                        file_name="盘点表_按库位.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                    st.success(f"已生成 {len(grouped)} 个sheet，按库位分组完成。")

                elif group_by == "按物料分组":
                    grouped = df.groupby(material_col)
                    buffer = io.BytesIO()
                    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                        for material, group in grouped:
                            sheet_name = str(material)[:31]
                            sheet_data = group[[name_col, qty_col]].copy()
                            sheet_data.rename(columns={
                                name_col: "货品名称",
                                qty_col: "账面库存"
                            }, inplace=True)
                            sheet_data["盘点数量"] = ""
                            sheet_data["备注"] = ""
                            sheet_data.to_excel(writer, sheet_name=sheet_name, index=False)
                    st.download_button(
                        label="📥 下载盘点表（按物料分页）",
                        data=buffer.getvalue(),
                        file_name="盘点表_按物料.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                    st.success(f"已生成 {len(grouped)} 个sheet，按物料分组完成。")

        except Exception as e:
            st.error(f"❌ 处理失败: {e}")
            st.exception(e)


# ========== 功能5：IB00库存匹配 ==========
def inventory_matching():
    st.header("📦 IB00工厂库存匹配")
    st.write("上传 IB00库存表 和 库位表，系统将自动匹配并生成盘存汇总表")
    
    # 文件上传
    col1, col2 = st.columns(2)
    
    with col1:
        ib00_file = st.file_uploader("上传 IB00库存表", type=['xlsx', 'xls', 'csv'], key="ib00")
        st.caption("需要包含：存储位置、非限制使用的库存、冻结库存等列")
    
    with col2:
        location_file = st.file_uploader("上传 库位表", type=['xlsx', 'xls', 'csv'], key="location")
        st.caption("需要包含：实物库位表和赠品库位表两个工作表")
    
    # 配置选项
    with st.expander("⚙️ 列名配置", expanded=False):
        st.info("请根据您的实际Excel列名修改以下配置")
        
        col_storage = st.text_input("存储位置列名", value="存储位置")
        col_unrestricted = st.text_input("非限制使用的库存列名", value="非限制使用的库存")
        col_frozen = st.text_input("冻结库存列名", value="冻结库存")
        col_loc_code = st.text_input("库位代码列名", value="库位代码")
        col_loc_desc = st.text_input("仓库描述列名", value="仓库描述")
        
        sheet_physical = st.text_input("实物库位表工作表名", value="实物库位表")
        sheet_gift = st.text_input("赠品库位表工作表名", value="赠品库位表")
    
    if st.button("🚀 开始匹配", type="primary", use_container_width=True):
        if ib00_file is None or location_file is None:
            st.error("❌ 请同时上传 IB00库存表 和 库位表")
        else:
            with st.spinner("⏳ 正在处理数据，请稍候..."):
                try:
                    # 读取文件
                    df_ib00 = read_file(ib00_file)
                    
                    # 读取库位表的两个工作表
                    df_physical = pd.read_excel(location_file, sheet_name=sheet_physical)
                    try:
                        df_gift = pd.read_excel(location_file, sheet_name=sheet_gift)
                    except:
                        df_gift = None
                        st.warning(f"⚠️ 未找到工作表 '{sheet_gift}'，将跳过赠品匹配")
                    
                    # 清洗数据
                    df_ib00[col_storage] = df_ib00[col_storage].astype(str).apply(clean_str)
                    df_ib00[col_unrestricted] = df_ib00[col_unrestricted].apply(clean_float)
                    df_ib00[col_frozen] = df_ib00[col_frozen].apply(clean_float)
                    
                    df_physical[col_loc_code] = df_physical[col_loc_code].astype(str).apply(clean_str)
                    if col_loc_desc in df_physical.columns:
                        df_physical[col_loc_desc] = df_physical[col_loc_desc].astype(str).apply(clean_str)
                    
                    if df_gift is not None:
                        df_gift[col_loc_code] = df_gift[col_loc_code].astype(str).apply(clean_str)
                        if col_loc_desc in df_gift.columns:
                            df_gift[col_loc_desc] = df_gift[col_loc_desc].astype(str).apply(clean_str)
                    
                    # 计算总库存
                    df_ib00['总库存'] = df_ib00[col_unrestricted] + df_ib00[col_frozen]
                    
                    # 创建匹配字典
                    loc_dict = {}
                    for _, row in df_physical.iterrows():
                        code = row[col_loc_code]
                        desc = row[col_loc_desc] if col_loc_desc in df_physical.columns else code
                        loc_dict[code] = desc
                    
                    gift_dict = {}
                    if df_gift is not None:
                        for _, row in df_gift.iterrows():
                            code = row[col_loc_code]
                            desc = row[col_loc_desc] if col_loc_desc in df_gift.columns else code
                            gift_dict[code] = desc
                    
                    # 成品匹配
                    df_ib00['仓库描述'] = ''
                    for idx, row in df_ib00.iterrows():
                        storage = row[col_storage]
                        if storage in loc_dict:
                            df_ib00.at[idx, '仓库描述'] = loc_dict[storage]
                    
                    # 成品汇总
                    product_summary = df_ib00[df_ib00['仓库描述'] != ''].groupby('仓库描述').agg(
                        总库存总和=('总库存', 'sum')
                    ).reset_index()
                    product_summary = product_summary.sort_values('仓库描述', ascending=True)
                    total_product = product_summary['总库存总和'].sum()
                    
                    # 赠品匹配（尾数6）
                    if df_gift is not None:
                        df_gift_stock = df_ib00[df_ib00[col_storage].str.endswith('6')].copy()
                        df_gift_stock['仓库描述'] = ''
                        for idx, row in df_gift_stock.iterrows():
                            storage = row[col_storage]
                            if storage in gift_dict:
                                df_gift_stock.at[idx, '仓库描述'] = gift_dict[storage]
                        
                        gift_summary = df_gift_stock[df_gift_stock['仓库描述'] != ''].groupby('仓库描述').agg(
                            总库存总和=('总库存', 'sum')
                        ).reset_index()
                        gift_summary = gift_summary.sort_values('仓库描述', ascending=True)
                        total_gift = gift_summary['总库存总和'].sum()
                    else:
                        gift_summary = pd.DataFrame()
                        total_gift = 0
                    
                    # 生成输出文件
                    output_buffer = io.BytesIO()
                    
                    with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
                        # 成品汇总表
                        product_data = product_summary[['仓库描述', '总库存总和']].copy()
                        product_data.columns = ['仓库描述', 'ERP账面数']
                        for col in ['入库未计数', '出库未记', '盘盈', '盘亏', '实盘', '备注']:
                            product_data[col] = ''
                        product_data.loc[len(product_data)] = ['合计', total_product, '', '', '', '', '', '']
                        product_data.to_excel(writer, sheet_name='成品汇总表', index=False, startrow=1)
                        
                        worksheet = writer.sheets['成品汇总表']
                        worksheet.cell(row=1, column=1, value='IB00工厂汇总\t2/3/6/7/8/Z库未计算在ERP账面数内')
                        
                        # 赠品汇总表
                        if not gift_summary.empty:
                            gift_data = gift_summary[['仓库描述', '总库存总和']].copy()
                            gift_data.columns = ['仓库描述', 'ERP账面数']
                            for col in ['入库未计数', '出库未记', '盘盈', '盘亏', '实盘', '备注']:
                                gift_data[col] = ''
                            gift_data.loc[len(gift_data)] = ['合计', total_gift, '', '', '', '', '', '']
                            gift_data.to_excel(writer, sheet_name='赠品汇总表', index=False, startrow=1)
                            
                            worksheet = writer.sheets['赠品汇总表']
                            worksheet.cell(row=1, column=1, value='IB00工厂汇总\t2/3/6/7/8/Z库未计算在ERP账面数内')
                        else:
                            pd.DataFrame({'说明': ['赠品库位表不存在或无尾数6记录']}).to_excel(writer, sheet_name='赠品汇总表', index=False)
                    
                    # 获取上个月
                    today = datetime.now()
                    if today.month == 1:
                        last_month = today.replace(year=today.year - 1, month=12)
                    else:
                        last_month = today.replace(month=today.month - 1)
                    last_month_name = last_month.strftime('%Y年%m月')
                    file_name = f'{last_month_name}美菱IB00工厂盘存数据、账外物资汇总.xlsx'
                    
                    # 提供下载
                    st.success("🎉 匹配完成！")
                    st.download_button(
                        label="📥 下载盘存汇总表",
                        data=output_buffer.getvalue(),
                        file_name=file_name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                    
                    # 显示预览
                    st.subheader("📊 成品汇总预览")
                    st.dataframe(product_data.head(20), use_container_width=True)
                    
                except Exception as e:
                    st.error(f"❌ 处理失败: {str(e)}")
                    st.exception(e)


# ========== 主界面：侧边栏导航 ==========
st.sidebar.title("📁 功能目录")
page = st.sidebar.radio(
    "请选择功能",
    ["库存对账工具", "库存查询", "核对盘存问题", "汇总盘点表", "盘点表基础数据制作", "IB00库存匹配"]
)

# ========== 根据用户选择渲染不同页面 ==========
if page == "库存对账工具":
    # ------------------- 原对账功能 -------------------
    st.title("📊 库存对账工具")
    st.markdown("请上传需要对账的文件，点击开始对账")

    with st.sidebar:
        st.header("⚙️ 配置选项")
        skip_rdc = st.checkbox("跳过 RDC 仓库匹配", value=False)
        st.markdown("---")
        st.markdown("### 📁 文件上传说明")
        st.info("支持 .xlsx、.xls、.csv 格式，每个文件限 200MB")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📂 源文件")
        wms_file = st.file_uploader("WMS 交易记录", type=['xlsx', 'xls', 'csv'], key="wms")
        r3_file = st.file_uploader("R3 交易记录", type=['xlsx', 'xls', 'csv'], key="r3")
        sales_file = st.file_uploader("销售下单异常报表 (可选)", type=['xlsx', 'xls', 'csv'], key="sales")

    with col2:
        st.subheader("📊 对比报表")
        target_file = st.file_uploader("WMS与R3库存差异报表", type=['xlsx', 'xls', 'csv'], key="target")
        rdc_file = st.file_uploader("RDC 仓库编号 (可选)", type=['xlsx', 'xls', 'csv'], key="rdc")

    if st.button("🚀 开始对账", type="primary", use_container_width=True):
        if wms_file is None or r3_file is None or target_file is None:
            st.error("❌ 请至少上传 WMS交易记录、R3交易记录 和 WMS与R3库存差异报表 三个文件")
        else:
            gc.collect()
            with st.spinner("⏳ 正在处理数据，请稍候..."):
                try:
                    df_wms = read_file(wms_file)
                    df_r3 = read_file(r3_file)
                    df_target = read_file(target_file)
                    df_sales = read_file(sales_file) if sales_file else None
                    df_rdc = read_file(rdc_file) if rdc_file else None

                    st.info(f"✅ 读取成功: WMS {len(df_wms)} 行, R3 {len(df_r3)} 行, 目标报表 {len(df_target)} 行")

                    df_wms_marked, df_summary, df_result = process_data(
                        df_wms, df_r3, df_sales, df_target, df_rdc, skip_rdc
                    )

                    del df_wms, df_r3, df_target
                    gc.collect()

                    if df_result is None:
                        st.warning("⚠️ 处理完成，但目标报表为空或处理失败")
                    else:
                        st.session_state['last_reconciliation_result'] = df_result
                        st.session_state['last_summary'] = df_summary
                        st.session_state['last_wms_marked'] = df_wms_marked

                        st.success("🎉 对账完成！")

                        st.subheader("📋 对账结果预览")
                        tab1, tab2, tab3 = st.tabs(["📄 未匹配汇总", "🏷️ 带标记的WMS表", "📈 最终差异报表"])

                        with tab1:
                            if df_summary is not None and not df_summary.empty:
                                st.dataframe(df_summary.head(20), use_container_width=True)
                                st.caption(f"共 {len(df_summary)} 行")
                            else:
                                st.info("无未匹配记录")

                        with tab2:
                            if df_wms_marked is not None and not df_wms_marked.empty:
                                st.dataframe(df_wms_marked.head(20), use_container_width=True)
                                st.caption(f"共 {len(df_wms_marked)} 行")
                            else:
                                st.info("无数据")

                        with tab3:
                            if df_result is not None and not df_result.empty:
                                st.dataframe(df_result.head(20), use_container_width=True)
                                st.caption(f"共 {len(df_result)} 行")
                            else:
                                st.info("无数据")

                        st.subheader("📥 下载结果")
                        col_d1, col_d2, col_d3 = st.columns(3)
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

                        with col_d1:
                            if df_summary is not None and not df_summary.empty:
                                buffer = io.BytesIO()
                                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                                    df_summary.to_excel(writer, sheet_name='未匹配汇总', index=False)
                                st.download_button(
                                    label="📄 下载未匹配汇总",
                                    data=buffer.getvalue(),
                                    file_name=f"未匹配汇总_{timestamp}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                )

                        with col_d2:
                            if df_wms_marked is not None and not df_wms_marked.empty:
                                buffer = io.BytesIO()
                                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                                    df_wms_marked.to_excel(writer, sheet_name='WMS交易记录_带匹配标记', index=False)
                                st.download_button(
                                    label="📄 下载带标记WMS表",
                                    data=buffer.getvalue(),
                                    file_name=f"WMS交易记录_带匹配标记_{timestamp}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                )

                        with col_d3:
                            if df_result is not None and not df_result.empty:
                                buffer = io.BytesIO()
                                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                                    df_result.to_excel(writer, sheet_name='库存差异报表_带未匹配单号', index=False)
                                st.download_button(
                                    label="📄 下载最终差异报表",
                                    data=buffer.getvalue(),
                                    file_name=f"库存差异报表_带未匹配单号_{timestamp}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                )

                except Exception as e:
                    st.error(f"❌ 处理失败: {str(e)}")
                    st.exception(e)

elif page == "库存查询":
    inventory_query()

elif page == "核对盘存问题":
    check_inventory_problems()

elif page == "汇总盘点表":
    summarize_inventory_sheets()

elif page == "盘点表基础数据制作":
    create_inventory_sheet()

elif page == "IB00库存匹配":
    inventory_matching()


# ========== 使用说明 ==========
with st.expander("📖 使用说明", expanded=False):
    st.markdown("""
    ### 📋 文件说明
    | 文件 | 必需 | 说明 |
    |------|------|------|
    | WMS交易记录 | ✅ | 包含 LRP单号、单号、货品编码、工厂、ERP库位、数量、进or出、保管员、交易类型 |
    | R3交易记录 | ✅ | 包含 前继单号、数量 |
    | WMS与R3库存差异报表 | ✅ | 包含 货品编号、工厂编码、库位编码、WMS和ERP的差异库存、仓库编码 |
    | 销售下单异常报表 | ❌ | 包含 运单号、返回消息 |
    | RDC仓库编号 | ❌ | 包含 仓库编号 |

    ### 🚀 操作步骤
    1. 在侧边栏选择功能
    2. 对于对账功能，上传所需文件，点击"开始对账"
    3. 预览结果并下载
    """)
