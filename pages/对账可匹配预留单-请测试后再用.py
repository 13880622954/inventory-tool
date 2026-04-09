# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import io
import gc
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
if 'reconciliation_done' not in st.session_state:
    st.session_state['reconciliation_done'] = False

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
COL_RESERVE_R3 = '预留编号'

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
    if s.lower() in ['nan', 'null', 'none', '']:
        return ''
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
    outbound_order = set(df_r3[df_r3[COL_QTY_R3] < 0][COL_ORDER_R3].astype(str).apply(clean_str))
    inbound_order = set(df_r3[df_r3[COL_QTY_R3] > 0][COL_ORDER_R3].astype(str).apply(clean_str))
    outbound_order.discard('')
    inbound_order.discard('')
    
    outbound_reserve = set()
    inbound_reserve = set()
    if COL_RESERVE_R3 in df_r3.columns:
        reserve_clean = df_r3[COL_RESERVE_R3].astype(str).apply(clean_str)
        outbound_reserve = set(reserve_clean[df_r3[COL_QTY_R3] < 0])
        inbound_reserve = set(reserve_clean[df_r3[COL_QTY_R3] > 0])
        outbound_reserve.discard('')
        inbound_reserve.discard('')
    
    return outbound_order, inbound_order, outbound_reserve, inbound_reserve

def process_data(df_wms, df_r3, df_sales, df_target, df_rdc, skip_rdc_match):
    outbound_order, inbound_order, outbound_reserve, inbound_reserve = get_r3_sets(df_r3)

    # 清洗WMS数据
    df_wms[COL_ORDER_WMS] = df_wms[COL_ORDER_WMS].astype(str).apply(clean_str)
    df_wms[COL_COMMON_NO] = df_wms[COL_COMMON_NO].astype(str).apply(clean_str)

    for col in [COL_MATERIAL_WMS, COL_PLANT_WMS, COL_STORAGE_WMS, COL_INOUT, COL_KEEPER, COL_TRANS_TYPE]:
        if col in df_wms.columns:
            df_wms[col] = df_wms[col].astype(str).apply(clean_str)

    df_wms[COL_QTY_WMS] = df_wms[COL_QTY_WMS].apply(clean_float)

    # 过滤有效交易记录
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

    # 匹配函数（原逻辑，保持不变）
    def match_outbound(row):
        lrp = row[COL_ORDER_WMS]
        if lrp != '':
            return '是' if lrp in outbound_order else '否'
        else:
            common = row[COL_COMMON_NO]
            if common != '':
                return '是' if common in outbound_reserve else '否'
            else:
                return '否'

    def match_inbound(row):
        lrp = row[COL_ORDER_WMS]
        if lrp != '':
            return '是' if lrp in inbound_order else '否'
        else:
            common = row[COL_COMMON_NO]
            if common != '':
                return '是' if common in inbound_reserve else '否'
            else:
                return '否'

    if not out_records.empty:
        out_records['匹配'] = out_records.apply(match_outbound, axis=1)
        unmatched_out = out_records[out_records['匹配'] == '否'].copy()
    else:
        unmatched_out = pd.DataFrame()

    if not receive_records.empty:
        receive_records['匹配'] = receive_records.apply(match_inbound, axis=1)
        unmatched_receive = receive_records[receive_records['匹配'] == '否'].copy()
    else:
        unmatched_receive = pd.DataFrame()

    if not cancel_records.empty:
        cancel_records['匹配'] = '否'
        unmatched_cancel = cancel_records.copy()
    else:
        unmatched_cancel = pd.DataFrame()

    # 数量调整：LRP单号为空时除以2（原逻辑，保持不变）
    def adjust_qty(row):
        if row[COL_ORDER_WMS] == '':
            return row[COL_QTY_WMS] / 2.0
        else:
            return row[COL_QTY_WMS]

    for df_temp in [unmatched_out, unmatched_receive, unmatched_cancel]:
        if not df_temp.empty:
            df_temp['有效单号'] = df_temp.apply(
                lambda row: row[COL_ORDER_WMS] if row[COL_ORDER_WMS] != '' else row[COL_COMMON_NO], axis=1
            )
            df_temp['数量_调整'] = df_temp.apply(adjust_qty, axis=1)

    # ========== 销售报表匹配：仅使用原始LRP单号（如果非空） ==========
    # 先为每个未匹配记录添加原始LRP列（用于销售匹配）
    for df_temp in [unmatched_out, unmatched_receive, unmatched_cancel]:
        if not df_temp.empty:
            df_temp['原始LRP'] = df_temp[COL_ORDER_WMS]  # 原始LRP单号（可能为空）

    if df_sales is not None and not df_sales.empty and COL_ORDER_SALES in df_sales.columns and COL_MSG_SALES in df_sales.columns:
        df_sales[COL_ORDER_SALES] = df_sales[COL_ORDER_SALES].astype(str).apply(clean_str)
        df_sales[COL_MSG_SALES] = df_sales[COL_MSG_SALES].astype(str).apply(clean_str)
        df_sales_unique = df_sales.drop_duplicates(subset=[COL_ORDER_SALES], keep='first')
        msg_map = dict(zip(df_sales_unique[COL_ORDER_SALES], df_sales_unique[COL_MSG_SALES]))
        # 为每条记录添加返回消息（基于原始LRP，若原始LRP为空则消息为空）
        for df_temp in [unmatched_out, unmatched_receive, unmatched_cancel]:
            if not df_temp.empty:
                df_temp['返回消息'] = df_temp['原始LRP'].map(msg_map).fillna('')
                df_temp['差异类型'] = df_temp['返回消息'].apply(get_diff_type)
    else:
        for df_temp in [unmatched_out, unmatched_receive, unmatched_cancel]:
            if not df_temp.empty:
                df_temp['返回消息'] = ''
                df_temp['差异类型'] = ''

    # 正负抵消（使用有效单号，原逻辑）
    if not unmatched_out.empty:
        out_agg = unmatched_out.groupby(['有效单号', COL_MATERIAL_WMS, COL_PLANT_WMS, COL_STORAGE_WMS]).agg(
            out_qty=('数量_调整', 'sum'),
            # 保留返回消息和差异类型（用于后续聚合）
            返回消息_list=('返回消息', lambda x: '\n'.join(sorted(set(x)))),
            差异类型_list=('差异类型', lambda x: ';'.join(sorted(set([v for v in x if v != '']))))
        ).reset_index()
    else:
        out_agg = pd.DataFrame()

    if not unmatched_cancel.empty:
        cancel_agg = unmatched_cancel.groupby('有效单号').agg(
            cancel_qty=('数量_调整', 'sum')
        ).reset_index()
    else:
        cancel_agg = pd.DataFrame()

    net_records = pd.DataFrame()
    if not out_agg.empty and not cancel_agg.empty:
        combined = pd.merge(out_agg, cancel_agg, on='有效单号', how='outer').fillna(0)
        combined['净数量'] = combined['out_qty'] - combined['cancel_qty']
        combined = combined[combined['净数量'] > 0]
        if not combined.empty:
            net_records = combined[['有效单号', '净数量', COL_MATERIAL_WMS, COL_PLANT_WMS, COL_STORAGE_WMS,
                                     '返回消息_list', '差异类型_list']].copy()
            net_records.rename(columns={'净数量': '数量_调整', '返回消息_list': '返回消息', '差异类型_list': '差异类型'}, inplace=True)
            net_records['记录类型'] = '出库'
        del combined
        gc.collect()
    elif not out_agg.empty:
        net_records = out_agg.copy()
        net_records.rename(columns={'out_qty': '数量_调整'}, inplace=True)
        net_records['记录类型'] = '出库'
        if '返回消息_list' in net_records.columns:
            net_records.rename(columns={'返回消息_list': '返回消息', '差异类型_list': '差异类型'}, inplace=True)

    if not unmatched_receive.empty:
        unmatched_receive['记录类型'] = '收货'
        receive_for_concat = unmatched_receive[['有效单号', '数量_调整', COL_MATERIAL_WMS, COL_PLANT_WMS, COL_STORAGE_WMS,
                                                 '记录类型', '返回消息', '差异类型']].copy()
    else:
        receive_for_concat = pd.DataFrame()

    all_unmatched = pd.concat([net_records, receive_for_concat], ignore_index=True)

    # 构建未匹配汇总表
    if all_unmatched.empty:
        df_summary = pd.DataFrame(columns=[COL_MATERIAL_WMS, COL_PLANT_WMS, COL_STORAGE_WMS,
                                           '未匹配单号列表', '未匹配单号个数', '数量', '返回消息', '差异类型',
                                           '出库数量', '收货数量'])
        df_wms_marked = pd.DataFrame()
    else:
        group_cols = [COL_MATERIAL_WMS, COL_PLANT_WMS, COL_STORAGE_WMS]
        # 汇总数量
        agg_qty = all_unmatched.groupby(group_cols + ['记录类型']).agg(
            数量=('数量_调整', 'sum')
        ).reset_index()
        pivot = agg_qty.pivot_table(index=group_cols, columns='记录类型', values='数量', fill_value=0).reset_index()
        if '出库' not in pivot.columns:
            pivot['出库'] = 0
        if '收货' not in pivot.columns:
            pivot['收货'] = 0
        pivot.rename(columns={'出库': '出库数量', '收货': '收货数量'}, inplace=True)

        # 聚合未匹配单号列表（有效单号）
        order_list_df = all_unmatched.groupby(group_cols)['有效单号'].apply(
            lambda x: '\n'.join(sorted(set(x.astype(str))))
        ).reset_index()
        order_list_df.rename(columns={'有效单号': '未匹配单号列表'}, inplace=True)
        order_list_df['未匹配单号个数'] = order_list_df['未匹配单号列表'].apply(lambda x: len(x.split('\n')) if x else 0)

        # 聚合返回消息和差异类型
        msg_agg = all_unmatched.groupby(group_cols).agg(
            返回消息=('返回消息', lambda x: '\n'.join(sorted(set(x.astype(str))))),
            差异类型=('差异类型', lambda x: ';'.join(sorted(set([v for v in x if v != '']))))
        ).reset_index()

        df_summary = pd.merge(pivot, order_list_df, on=group_cols, how='left')
        df_summary = pd.merge(df_summary, msg_agg, on=group_cols, how='left')
        df_summary['数量'] = df_summary['出库数量'].astype(int)
        # 填充空值
        df_summary['返回消息'] = df_summary['返回消息'].fillna('')
        df_summary['差异类型'] = df_summary['差异类型'].fillna('')

        # 构建带标记的WMS表
        all_matched = pd.concat([
            out_records[out_records['匹配'] == '是'] if not out_records.empty else pd.DataFrame(),
            receive_records[receive_records['匹配'] == '是'] if not receive_records.empty else pd.DataFrame()
        ], ignore_index=True)
        all_unmatched_temp = pd.concat([unmatched_out, unmatched_cancel, unmatched_receive], ignore_index=True)
        if not all_matched.empty and '有效单号' not in all_matched.columns:
            all_matched['有效单号'] = all_matched.apply(
                lambda row: row[COL_ORDER_WMS] if row[COL_ORDER_WMS] != '' else row[COL_COMMON_NO], axis=1
            )
            all_matched['数量_调整'] = all_matched.apply(adjust_qty, axis=1)
        df_wms_marked = pd.concat([all_matched, all_unmatched_temp], ignore_index=True)

    # 释放内存
    del out_records, cancel_records, receive_records
    del unmatched_out, unmatched_cancel, unmatched_receive
    gc.collect()

    # 处理目标报表（与原逻辑一致）
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

# ========== 主界面 ==========
st.sidebar.title("📁 功能目录")
page = st.sidebar.radio("请选择功能", ["库存对账工具"])

if page == "库存对账工具":
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
                        st.session_state['reconciliation_done'] = True
                        st.success("🎉 对账完成！")

                except Exception as e:
                    st.error(f"❌ 处理失败: {str(e)}")
                    st.exception(e)

    # 显示结果
    if st.session_state['reconciliation_done']:
        df_result = st.session_state['last_reconciliation_result']
        df_summary = st.session_state['last_summary']
        df_wms_marked = st.session_state['last_wms_marked']

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

# 使用说明
with st.expander("📖 使用说明", expanded=False):
    st.markdown("""
    ### 📋 库存对账工具文件说明
    | 文件 | 必需 | 说明 |
    |------|------|------|
    | WMS交易记录 | ✅ | 包含 LRP单号、单号、货品编码、工厂、ERP库位、数量、进or出、保管员、交易类型 |
    | R3交易记录 | ✅ | 包含 前继单号、数量、预留编号 |
    | WMS与R3库存差异报表 | ✅ | 包含 货品编号、工厂编码、库位编码、WMS和ERP的差异库存、仓库编码 |
    | 销售下单异常报表 | ❌ | 包含 运单号、返回消息 |
    | RDC仓库编号 | ❌ | 包含 仓库编号 |

    ### 🚀 操作步骤
    1. 上传所需文件，点击"开始对账"
    2. 预览结果并下载

    ### 💡 特殊逻辑说明
    - 当 LRP 单号为空时，系统会使用“单号”列匹配 R3 的“预留编号”，并且该记录的数量会自动除以2。
    - 销售下单异常报表的“运单号”只与 WMS 的 LRP 单号关联，不会使用“单号”匹配。
    - 未匹配汇总表中的数量为调整后的数量（LRP为空的记录已折半）。
    """)
