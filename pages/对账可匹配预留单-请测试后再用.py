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
    '出具发票方与付款方必须一致': '门店代码未维护',
    '已创建': '已处理',
    '初始定价类型': '定价过程未配置'
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

def normalize_number(s):
    """去除字符串的前导0，用于匹配预留编号"""
    s = s.strip()
    if s == '' or s == '0':
        return s
    return s.lstrip('0')

def is_start_with_4(s):
    """判断字符串是否以'4'开头（用于过滤单号）"""
    s = str(s).strip()
    return s.startswith('4')

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
        outbound_reserve = set(normalize_number(v) for v in reserve_clean[df_r3[COL_QTY_R3] < 0] if v != '')
        inbound_reserve = set(normalize_number(v) for v in reserve_clean[df_r3[COL_QTY_R3] > 0] if v != '')
        outbound_reserve.discard('')
        inbound_reserve.discard('')
    
    return outbound_order, inbound_order, outbound_reserve, inbound_reserve

def process_data(df_wms, df_r3, df_sales, df_target, df_rdc, skip_rdc_match):
    outbound_order, inbound_order, outbound_reserve, inbound_reserve = get_r3_sets(df_r3)

    # ========== 清洗WMS数据 ==========
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

    # ========== 匹配函数（与R3交易记录匹配） ==========
    def match_outbound(row):
        lrp = row[COL_ORDER_WMS]
        if lrp != '':
            return '是' if lrp in outbound_order else '否'
        else:
            common = row[COL_COMMON_NO]
            if common != '' and not is_start_with_4(common):
                common_norm = normalize_number(common)
                return '是' if common_norm in outbound_reserve else '否'
            else:
                return '否'

    def match_inbound(row):
        lrp = row[COL_ORDER_WMS]
        if lrp != '':
            return '是' if lrp in inbound_order else '否'
        else:
            common = row[COL_COMMON_NO]
            if common != '' and not is_start_with_4(common):
                common_norm = normalize_number(common)
                return '是' if common_norm in inbound_reserve else '否'
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

    # ========== 数量调整（仅出库且LRP为空时除以2） ==========
    def adjust_qty(row, record_type):
        if record_type == 'out' and row[COL_ORDER_WMS] == '':
            return row[COL_QTY_WMS] / 2.0
        else:
            return row[COL_QTY_WMS]

    if not unmatched_out.empty:
        unmatched_out['数量_调整'] = unmatched_out.apply(lambda r: adjust_qty(r, 'out'), axis=1)
        unmatched_out['记录类型'] = '出库'
    if not unmatched_cancel.empty:
        unmatched_cancel['数量_调整'] = unmatched_cancel[COL_QTY_WMS]  # 取消出库不除以2
        unmatched_cancel['记录类型'] = '取消出库'
    if not unmatched_receive.empty:
        unmatched_receive['数量_调整'] = unmatched_receive[COL_QTY_WMS]  # 收货不除以2
        unmatched_receive['记录类型'] = '收货'

    # ========== 生成有效单号（用于展示，排除以4开头） ==========
    def get_effective_order(row):
        lrp = row[COL_ORDER_WMS]
        if lrp != '':
            return lrp
        else:
            common = row[COL_COMMON_NO]
            if common != '' and not is_start_with_4(common):
                return common
            else:
                return ''

    for df_temp in [unmatched_out, unmatched_cancel, unmatched_receive]:
        if not df_temp.empty:
            df_temp['有效单号'] = df_temp.apply(get_effective_order, axis=1)
            df_temp['原始LRP'] = df_temp[COL_ORDER_WMS]

    # 过滤掉有效单号为空的记录（以4开头的单号不参与后续）
    unmatched_out = unmatched_out[unmatched_out['有效单号'] != ''] if not unmatched_out.empty else unmatched_out
    unmatched_cancel = unmatched_cancel[unmatched_cancel['有效单号'] != ''] if not unmatched_cancel.empty else unmatched_cancel
    unmatched_receive = unmatched_receive[unmatched_receive['有效单号'] != ''] if not unmatched_receive.empty else unmatched_receive

    # ========== 销售报表匹配 ==========
    if df_sales is not None and not df_sales.empty and COL_ORDER_SALES in df_sales.columns and COL_MSG_SALES in df_sales.columns:
        df_sales[COL_ORDER_SALES] = df_sales[COL_ORDER_SALES].astype(str).apply(clean_str)
        df_sales[COL_MSG_SALES] = df_sales[COL_MSG_SALES].astype(str).apply(clean_str)
        df_sales_unique = df_sales.drop_duplicates(subset=[COL_ORDER_SALES], keep='first')
        msg_map = dict(zip(df_sales_unique[COL_ORDER_SALES], df_sales_unique[COL_MSG_SALES]))
        for df_temp in [unmatched_out, unmatched_cancel, unmatched_receive]:
            if not df_temp.empty:
                df_temp['返回消息'] = df_temp['原始LRP'].apply(lambda x: msg_map.get(x, '') if x != '' else '')
                df_temp['差异类型'] = df_temp['返回消息'].apply(get_diff_type)
    else:
        for df_temp in [unmatched_out, unmatched_cancel, unmatched_receive]:
            if not df_temp.empty:
                df_temp['返回消息'] = ''
                df_temp['差异类型'] = ''

    # ========== 出库与取消出库按四字段完全抵消 ==========
    key_cols = [COL_COMMON_NO, COL_MATERIAL_WMS, COL_PLANT_WMS, COL_STORAGE_WMS]

    # 出库汇总
    if not unmatched_out.empty:
        out_sum = unmatched_out.groupby(key_cols)['数量_调整'].sum().reset_index(name='出库数量')
    else:
        out_sum = pd.DataFrame(columns=key_cols + ['出库数量'])

    # 取消出库汇总
    if not unmatched_cancel.empty:
        cancel_sum = unmatched_cancel.groupby(key_cols)['数量_调整'].sum().reset_index(name='取消出库数量')
    else:
        cancel_sum = pd.DataFrame(columns=key_cols + ['取消出库数量'])

    # 合并计算净出库数量
    net_out = pd.merge(out_sum, cancel_sum, on=key_cols, how='outer').fillna(0)
    net_out['出库净数量'] = net_out['出库数量'] - net_out['取消出库数量']
    net_out = net_out[net_out['出库净数量'] > 0].copy()  # 只保留净数量>0的组（未被完全抵消）

    # 为净剩余的组提取有效单号列表及消息
    if not net_out.empty:
        # 将原始出库记录关联回这些组
        out_with_keys = unmatched_out.copy()
        out_with_keys['_key'] = out_with_keys[key_cols].apply(tuple, axis=1)
        net_out['_key'] = net_out[key_cols].apply(tuple, axis=1)

        # 单号列表
        single_list = out_with_keys.groupby('_key')['有效单号'].apply(lambda x: '\n'.join(sorted(set(x)))).reset_index()
        single_list.columns = ['_key', '未匹配单号列表']
        single_list['未匹配单号个数'] = single_list['未匹配单号列表'].apply(lambda x: len(x.split('\n')) if x else 0)

        # 返回消息和差异类型（取该组所有非空消息去重拼接）
        msg_agg = out_with_keys.groupby('_key').agg({
            '返回消息': lambda x: '\n'.join(sorted(set([v for v in x if v != '']))),
            '差异类型': lambda x: ';'.join(sorted(set([v for v in x if v != ''])))
        }).reset_index()
        msg_agg.columns = ['_key', '返回消息', '差异类型']

        net_out = net_out.merge(single_list, on='_key', how='left')
        net_out = net_out.merge(msg_agg, on='_key', how='left')
        net_out.drop(columns=['_key'], inplace=True)

        net_out['记录类型'] = '出库净剩余'
    else:
        net_out = pd.DataFrame()

    # ========== 处理收货记录（无抵消，直接汇总） ==========
    if not unmatched_receive.empty:
        receive_with_keys = unmatched_receive.copy()
        receive_with_keys['_key'] = receive_with_keys[key_cols].apply(tuple, axis=1)

        receive_agg = receive_with_keys.groupby('_key').agg({
            '数量_调整': 'sum',
            '有效单号': lambda x: '\n'.join(sorted(set(x))),
            '返回消息': lambda x: '\n'.join(sorted(set([v for v in x if v != '']))),
            '差异类型': lambda x: ';'.join(sorted(set([v for v in x if v != ''])))
        }).reset_index()
        receive_agg.rename(columns={'数量_调整': '收货净数量', '有效单号': '收货单号列表'}, inplace=True)
        receive_agg['收货单号个数'] = receive_agg['收货单号列表'].apply(lambda x: len(x.split('\n')) if x else 0)

        # 恢复原始列
        receive_agg[key_cols] = pd.DataFrame(receive_agg['_key'].tolist(), index=receive_agg.index)
        receive_agg.drop(columns=['_key'], inplace=True)
        receive_agg['记录类型'] = '收货'
    else:
        receive_agg = pd.DataFrame()

    # ========== 合并出库净剩余与收货，生成最终汇总表 ==========
    all_unmatched_agg = pd.concat([net_out, receive_agg], ignore_index=True, sort=False)

    if all_unmatched_agg.empty:
        df_summary = pd.DataFrame(columns=[COL_MATERIAL_WMS, COL_PLANT_WMS, COL_STORAGE_WMS,
                                           '未匹配单号列表', '未匹配单号个数', '数量', '返回消息', '差异类型',
                                           '出库数量', '收货数量'])
    else:
        group_cols = [COL_MATERIAL_WMS, COL_PLANT_WMS, COL_STORAGE_WMS]

        # 二次聚合（同一物料+工厂+库位可能既有出库净剩余又有收货）
        final_agg = all_unmatched_agg.groupby(group_cols).agg({
            '出库净数量': lambda x: x.fillna(0).sum(),
            '收货净数量': lambda x: x.fillna(0).sum(),
            '未匹配单号列表': lambda x: '\n'.join(sorted(set('\n'.join(x.fillna('')).split('\n')))).strip('\n'),
            '收货单号列表': lambda x: '\n'.join(sorted(set('\n'.join(x.fillna('')).split('\n')))).strip('\n'),
            '返回消息': lambda x: '\n'.join(sorted(set('\n'.join(x.fillna('')).split('\n')))).strip('\n'),
            '差异类型': lambda x: ';'.join(sorted(set(';'.join(x.fillna('')).split(';')))).strip(';')
        }).reset_index()

        # 合并出库和收货的单号列表
        final_agg['未匹配单号列表'] = (final_agg['未匹配单号列表'].fillna('') + '\n' + final_agg['收货单号列表'].fillna('')).str.strip('\n')
        final_agg['未匹配单号个数'] = final_agg['未匹配单号列表'].apply(lambda x: len(x.split('\n')) if x else 0)

        final_agg['数量'] = final_agg['出库净数量'] + final_agg['收货净数量']

        df_summary = final_agg[group_cols + ['未匹配单号列表', '未匹配单号个数', '数量', '返回消息', '差异类型', '出库净数量', '收货净数量']]
        df_summary.rename(columns={'出库净数量': '出库数量', '收货净数量': '收货数量'}, inplace=True)

    # ========== 构建带标记的WMS表（用于展示，包含所有未匹配原始记录） ==========
    # 已匹配的记录
    all_matched = pd.concat([
        out_records[out_records['匹配'] == '是'] if not out_records.empty else pd.DataFrame(),
        receive_records[receive_records['匹配'] == '是'] if not receive_records.empty else pd.DataFrame()
    ], ignore_index=True)

    # 为已匹配记录也添加有效单号和调整数量
    if not all_matched.empty:
        def get_effective_order_matched(row):
            lrp = row[COL_ORDER_WMS]
            if lrp != '':
                return lrp
            else:
                common = row[COL_COMMON_NO]
                if common != '' and not is_start_with_4(common):
                    return common
                else:
                    return ''
        all_matched['有效单号'] = all_matched.apply(get_effective_order_matched, axis=1)
        all_matched['数量_调整'] = all_matched.apply(lambda r: adjust_qty(r, 'out') if r[COL_TRANS_TYPE]=='出库' else r[COL_QTY_WMS], axis=1)
        all_matched['记录类型'] = all_matched[COL_TRANS_TYPE]
        all_matched['返回消息'] = ''
        all_matched['差异类型'] = ''
        all_matched['原始LRP'] = all_matched[COL_ORDER_WMS]

    # 合并所有未匹配记录（出库、取消出库、收货）
    unmatched_all = pd.concat([unmatched_out, unmatched_cancel, unmatched_receive], ignore_index=True)
    df_wms_marked = pd.concat([all_matched, unmatched_all], ignore_index=True)

    # ========== 处理目标报表 ==========
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
            # 构建summary字典用于快速查找
            summary_dict = {}
            for _, row in df_summary.iterrows():
                key = (row[COL_MATERIAL_WMS], row[COL_PLANT_WMS], row[COL_STORAGE_WMS])
                summary_dict[key] = {
                    '未匹配单号列表': row['未匹配单号列表'],
                    '未匹配单号个数': row['未匹配单号个数'],
                    '数量': row['数量'],
                    '返回消息': row['返回消息'],
                    '差异类型': row['差异类型'],
                    '出库数量': row['出库数量'],
                    '收货数量': row['收货数量']
                }

            # 为目标报表添加列
            df_target['未匹配单号列表'] = ''
            df_target['未匹配单号个数'] = 0
            df_target['数量'] = 0
            df_target['返回消息'] = ''
            df_target['差异类型'] = ''
            df_target['调整后差异'] = 0.0

            for idx, row in df_target.iterrows():
                key = (row[COL_MATERIAL_TARGET], row[COL_PLANT_TARGET], row[COL_STORAGE_TARGET])
                if key in summary_dict:
                    rec = summary_dict[key]
                    df_target.at[idx, '未匹配单号列表'] = rec['未匹配单号列表']
                    df_target.at[idx, '未匹配单号个数'] = rec['未匹配单号个数']
                    df_target.at[idx, '数量'] = rec['数量']
                    df_target.at[idx, '返回消息'] = rec['返回消息']
                    df_target.at[idx, '差异类型'] = rec['差异类型']

                    diff_val = row[COL_DIFF_TARGET]
                    net_qty = rec['数量']  # 出库净数量 + 收货净数量

                    # 按最初规则：正差异减，负差异加
                    if diff_val > 0:
                        adjusted = diff_val - net_qty
                    elif diff_val < 0:
                        adjusted = diff_val + net_qty
                    else:
                        adjusted = -net_qty
                    df_target.at[idx, '调整后差异'] = adjusted
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
    | WMS交易记录 | ✅ | 导表区域-WMS系统-库存交易记录-成品导出\异步导出-重命名为WMS交易记录。包含 LRP单号、单号、货品编码、工厂、ERP库位、数量、进or出、保管员、交易类型 |
    | R3交易记录 | ✅ |  导表区域-R3系统-ZMB51_RE-左上角列表-导出-电子表格重命名未R3交易记录。包含 前继单号、数量、预留编号 |
    | WMS与R3库存差异报表 | ✅ | 导表区域-WMS系统-机号码库存差异查询，不更改任何条件导出，只导美菱货主选择IB00。导出重命名为WMS与R3库存差异报表。包含 货品编号、工厂编码、库位编码、WMS和ERP的差异库存、仓库编码 |
    | 销售下单异常报表 | ❌ |导表区域-ZDDGL销售区域-运单号粘贴WMS全部L单号（去重后的L单号）导出重命名为销售下单异常报表。 包含 运单号、返回消息 |
    | RDC仓库编号 | ❌ | 包含 仓库编号 |

    ### 🚀 操作步骤
    1. 上传所需文件，点击"开始对账"
    2. 预览结果并下载

    ### 💡 特殊逻辑说明
    - **R3匹配**：LRP单号非空时用LRP匹配前继单号；LRP为空且单号不以4开头时，用单号（去除前导0）匹配预留编号；单号以4开头的直接视为未匹配且不参与后续汇总。
    - **销售报表匹配**：仅当LRP单号非空时，用LRP匹配运单号；LRP为空时销售消息为空。
    - **数量调整**：仅当出库记录且LRP单号为空时，数量自动除以2；取消出库和收货不除。
    - **取消出库抵消**：按 单号+货品编码+工厂+ERP库位 四字段完全匹配，出库数量减去取消出库数量，净数量>0才保留，已完全抵消的组不出现任何单号。
    - **未匹配汇总**：使用“有效单号”（LRP优先，否则取不以4开头的单号）作为单号列表。
    - **调整后差异**：正差异减去净未匹配量，负差异加上净未匹配量。
    """)
