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
if 'pending_wms' not in st.session_state:
    st.session_state['pending_wms'] = None
if 'pending_r3' not in st.session_state:
    st.session_state['pending_r3'] = None
if 'pending_target' not in st.session_state:
    st.session_state['pending_target'] = None
if 'pending_sales' not in st.session_state:
    st.session_state['pending_sales'] = None
if 'pending_rdc' not in st.session_state:
    st.session_state['pending_rdc'] = None
if 'pending_skip_rdc' not in st.session_state:
    st.session_state['pending_skip_rdc'] = False
if 'need_po_map' not in st.session_state:
    st.session_state['need_po_map'] = False
if 'template_df' not in st.session_state:
    st.session_state['template_df'] = None

# ========== 自定义CSS（全局美化，不动功能） ==========
st.markdown("""
<style>
    .stApp {
        background: linear-gradient(145deg, #f8fafc 0%, #f1f5f9 100%) !important;
        font-family: 'Inter', 'Segoe UI', 'Microsoft YaHei', sans-serif !important;
    }
    [data-testid="stSidebar"] {
        background: #ffffff !important;
        border-right: 1px solid #e2e8f0 !important;
        box-shadow: 2px 0 12px rgba(0,0,0,0.02) !important;
    }
    [data-testid="stSidebar"] .stRadio label {
        border-radius: 10px !important;
        padding: 0.5rem 1rem !important;
        transition: all 0.15s !important;
    }
    [data-testid="stSidebar"] .stRadio label:hover {
        background: #f1f5f9 !important;
    }
    [data-testid="stSidebar"] .stRadio label[data-selected="true"] {
        background: #eef2ff !important;
        color: #2563eb !important;
        font-weight: 500 !important;
    }
    h1 {
        color: #1e293b !important;
        font-weight: 700 !important;
        border-bottom: 3px solid #2563eb !important;
        display: inline-block !important;
        padding-bottom: 0.25rem !important;
    }
    h2, h3 {
        color: #334155 !important;
        font-weight: 600 !important;
    }
    .stVerticalBlock > div[data-testid="stVerticalBlock"] {
        background: #ffffff !important;
        border-radius: 20px !important;
        padding: 1.5rem 1.8rem !important;
        box-shadow: 0 8px 20px -6px rgba(0,0,0,0.05), 0 2px 6px rgba(0,0,0,0.02) !important;
        border: 1px solid #eef2f6 !important;
        margin-bottom: 1.5rem !important;
    }
    .stButton > button {
        background: linear-gradient(145deg, #2563eb, #1d4ed8) !important;
        color: white !important;
        border: none !important;
        border-radius: 14px !important;
        padding: 0.6rem 1.8rem !important;
        font-weight: 500 !important;
        box-shadow: 0 4px 8px -2px rgba(37,99,235,0.3) !important;
        transition: all 0.2s ease !important;
    }
    .stButton > button:hover {
        background: linear-gradient(145deg, #1d4ed8, #1e40af) !important;
        transform: translateY(-1px) !important;
        box-shadow: 0 8px 14px -4px rgba(37,99,235,0.4) !important;
    }
    div[data-testid="stHorizontalBlock"] .stButton:not(:first-child) > button {
        background: #ffffff !important;
        color: #475569 !important;
        border: 1px solid #cbd5e1 !important;
        box-shadow: none !important;
    }
    div[data-testid="stHorizontalBlock"] .stButton:not(:first-child) > button:hover {
        background: #f8fafc !important;
        border-color: #94a3b8 !important;
        transform: none !important;
    }
    [data-testid="stFileUploader"] section {
        border: 2px dashed #cbd5e1 !important;
        border-radius: 24px !important;
        background: #fafcff !important;
        padding: 1.2rem 1.5rem !important;
        transition: all 0.2s !important;
    }
    [data-testid="stFileUploader"] section:hover {
        border-color: #2563eb !important;
        background: #ffffff !important;
        box-shadow: 0 4px 10px rgba(37,99,235,0.05) !important;
    }
    [data-testid="stDataFrame"] {
        border-radius: 16px !important;
        border: 1px solid #e2e8f0 !important;
        overflow: hidden !important;
    }
    [data-testid="stDataFrame"] th {
        background: #f8fafc !important;
        color: #0f172a !important;
        font-weight: 600 !important;
        padding: 0.8rem 1rem !important;
    }
    [data-testid="stDataFrame"] td {
        padding: 0.65rem 1rem !important;
        border-bottom: 1px solid #f1f5f9 !important;
    }
    [data-testid="stDownloadButton"] > button {
        background: white !important;
        color: #2563eb !important;
        border: 1.5px solid #2563eb !important;
        border-radius: 12px !important;
        font-weight: 500 !important;
        box-shadow: none !important;
    }
    [data-testid="stDownloadButton"] > button:hover {
        background: #eff6ff !important;
        border-color: #1e40af !important;
    }
    [data-testid="stAlert"] {
        border-radius: 16px !important;
        border-left-width: 8px !important;
        box-shadow: 0 2px 8px rgba(0,0,0,0.02) !important;
    }
    details {
        background: #ffffff !important;
        border-radius: 18px !important;
        border: 1px solid #eef2f6 !important;
        padding: 1.2rem 1.8rem !important;
        margin-top: 2.5rem !important;
        box-shadow: 0 4px 12px rgba(0,0,0,0.02) !important;
    }
    details summary {
        font-weight: 650 !important;
        color: #1e293b !important;
    }
    [data-testid="stTabs"] button {
        border-radius: 14px 14px 0 0 !important;
        padding: 0.6rem 1.8rem !important;
        font-weight: 500 !important;
        background: transparent !important;
        color: #64748b !important;
    }
    [data-testid="stTabs"] button[aria-selected="true"] {
        background: #ffffff !important;
        color: #2563eb !important;
        border-bottom: 3px solid #2563eb !important;
    }
    [data-testid="stMetric"] {
        background: #ffffff !important;
        border-radius: 18px !important;
        padding: 1.2rem 1.5rem !important;
        box-shadow: 0 4px 10px rgba(0,0,0,0.02) !important;
        border: 1px solid #f0f4f9 !important;
    }
</style>
""", unsafe_allow_html=True)

# ========== 列名配置 ==========
# WMS
COL_ORDER_WMS = 'LRP单号'
COL_COMMON_NO = '单号'
COL_MATERIAL_WMS = '货品编码'
COL_PLANT_WMS = '工厂'
COL_STORAGE_WMS = 'ERP库位'
COL_QTY_WMS = '数量'
COL_INOUT = '进or出'
COL_KEEPER = '保管员'
COL_TRANS_TYPE = '交易类型'

# R3（请根据实际列名修改）
COL_ORDER_R3 = '前继单号'
COL_QTY_R3 = '数量'
COL_RESERVE_R3 = '预留编号'
COL_MATERIAL_R3 = '物料代码'      # R3物料编码列名
COL_PLANT_R3 = '工厂'            # R3工厂列名
COL_STORAGE_R3 = '库位'          # R3库位列名
COL_PO_R3 = '采购订单'           # R3采购订单列名

# 销售报表
COL_ORDER_SALES = '运单号'
COL_MSG_SALES = '返回消息'

# 目标报表
COL_MATERIAL_TARGET = '货品编号'
COL_PLANT_TARGET = '工厂编码'
COL_STORAGE_TARGET = '库位编码'
COL_DIFF_TARGET = 'WMS和ERP的差异库存'
COL_WAREHOUSE_TARGET = '仓库编码'

COL_RDC_WAREHOUSE = '仓库编号'

# 映射模板列名
COL_TEMPLATE_PO = '采购订单'
COL_TEMPLATE_OLD_ORDER = '原前继单号'
COL_TEMPLATE_LRP = '对应LRP单号'

KEYWORD_DIFF_TYPE = {
    '超账期冻结': '客户代码冻结',
    '定价错误': '价格未维护',
    '出具发票方与付款方必须一致': '门店代码未维护',
    '已创建': '已处理',
    '初始定价类型': '定价过程未配置'
}

# ========== 辅助函数 ==========
def clean_str(val):
    if pd.isna(val): return ''
    s = str(val).strip()
    if s.lower() in ['nan', 'null', 'none', '']: return ''
    try:
        if '.' in s:
            f = float(s)
            if f.is_integer(): s = str(int(f))
    except: pass
    return s

def normalize_number(s):
    s = s.strip()
    if s == '' or s == '0': return s
    return s.lstrip('0')

def is_start_with_4(s):
    return str(s).strip().startswith('4')

def is_start_with_01(s):
    return str(s).strip().startswith('01')

def clean_float(val):
    try: return float(val)
    except: return 0.0

def get_diff_type(msg):
    if pd.isna(msg) or msg == '': return ''
    types = []
    for k, v in KEYWORD_DIFF_TYPE.items():
        if k in msg: types.append(v)
    return ';'.join(list(dict.fromkeys(types)))

def read_file(file):
    if file is None: return None
    try:
        if file.name.endswith('.csv'): return pd.read_csv(file, encoding='utf-8-sig')
        elif file.name.endswith('.xls'): return pd.read_excel(file, engine='xlrd')
        else: return pd.read_excel(file, engine='openpyxl')
    except:
        try: return pd.read_excel(file)
        except:
            file.seek(0)
            return pd.read_csv(file, encoding='utf-8-sig')

@st.cache_data
def get_r3_sets(df_r3):
    df_r3['_mat'] = df_r3[COL_MATERIAL_R3].astype(str).apply(clean_str)
    df_r3['_plant'] = df_r3[COL_PLANT_R3].astype(str).apply(clean_str)
    df_r3['_storage'] = df_r3[COL_STORAGE_R3].astype(str).apply(clean_str)
    df_r3['_order'] = df_r3[COL_ORDER_R3].astype(str).apply(clean_str)
    df_r3['_reserve'] = df_r3[COL_RESERVE_R3].astype(str).apply(clean_str).apply(normalize_number)
    df_r3['_po'] = df_r3[COL_PO_R3].astype(str).apply(clean_str)

    out_mask = df_r3[COL_QTY_R3] < 0
    in_mask = df_r3[COL_QTY_R3] > 0

    outbound_order = set()
    inbound_order = set()
    for _, row in df_r3[out_mask].iterrows():
        if row['_order'] != '':
            outbound_order.add((row['_order'], row['_mat'], row['_plant'], row['_storage']))
    for _, row in df_r3[in_mask].iterrows():
        if row['_order'] != '':
            inbound_order.add((row['_order'], row['_mat'], row['_plant'], row['_storage']))

    outbound_reserve = set()
    inbound_reserve = set()
    for _, row in df_r3[out_mask].iterrows():
        if row['_reserve'] != '':
            outbound_reserve.add((row['_reserve'], row['_mat'], row['_plant'], row['_storage']))
    for _, row in df_r3[in_mask].iterrows():
        if row['_reserve'] != '':
            inbound_reserve.add((row['_reserve'], row['_mat'], row['_plant'], row['_storage']))

    outbound_po = set()
    inbound_po = set()
    for _, row in df_r3[out_mask].iterrows():
        if row['_po'] != '':
            outbound_po.add((row['_po'], row['_mat'], row['_plant'], row['_storage']))
    for _, row in df_r3[in_mask].iterrows():
        if row['_po'] != '':
            inbound_po.add((row['_po'], row['_mat'], row['_plant'], row['_storage']))

    return (outbound_order, inbound_order, outbound_reserve, inbound_reserve,
            outbound_po, inbound_po)

def apply_po_mapping(df_r3, df_map):
    if df_map is None or df_map.empty:
        return df_r3
    required = [COL_TEMPLATE_OLD_ORDER, COL_TEMPLATE_LRP]
    for col in required:
        if col not in df_map.columns:
            st.error(f"映射表必须包含 '{col}' 列")
            return df_r3
    df_map = df_map.copy()
    df_map[COL_TEMPLATE_OLD_ORDER] = df_map[COL_TEMPLATE_OLD_ORDER].astype(str).apply(clean_str)
    df_map[COL_TEMPLATE_LRP] = df_map[COL_TEMPLATE_LRP].astype(str).apply(clean_str)
    df_map = df_map[df_map[COL_TEMPLATE_LRP] != '']
    if df_map.empty:
        st.warning("映射表中没有有效的 LRP 单号，将使用原单号对账。")
        return df_r3
    mapping = dict(zip(df_map[COL_TEMPLATE_OLD_ORDER], df_map[COL_TEMPLATE_LRP]))
    df_r3_copy = df_r3.copy()
    replaced = 0
    for idx, row in df_r3_copy.iterrows():
        order = str(row[COL_ORDER_R3]).strip()
        if order in mapping:
            df_r3_copy.at[idx, COL_ORDER_R3] = mapping[order]
            replaced += 1
    if replaced > 0:
        st.success(f"已使用映射表替换 {replaced} 条前继单号")
    return df_r3_copy

def process_data(df_wms, df_r3, df_sales, df_target, df_rdc, skip_rdc_match):
    sets = get_r3_sets(df_r3)
    (outbound_order, inbound_order, outbound_reserve, inbound_reserve,
     outbound_po, inbound_po) = sets

    # ========== 清洗WMS数据 ==========
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

    # 需要保留系统API保管员的收货记录条件：LRP空，单号不以01/4开头
    keep_api_condition = (
        (df_wms[COL_INOUT] == 'IN') &
        (df_wms[COL_TRANS_TYPE] == '收货') &
        (df_wms[COL_ORDER_WMS].astype(str).apply(clean_str) == '') &
        (~df_wms[COL_COMMON_NO].astype(str).apply(clean_str).str.startswith('01')) &
        (~df_wms[COL_COMMON_NO].astype(str).apply(clean_str).str.startswith('4'))
    )

    # 需要删除的系统API记录：出库/取消出库，以及不符合上述条件的收货
    delete_api_condition = (
        (df_wms[COL_KEEPER] == '系统API') &
        ~keep_api_condition
    )

    df_wms = df_wms[~delete_api_condition]

    if df_wms.empty:
        return None, None, None

    out_records = df_wms[(df_wms[COL_INOUT] == 'OUT') & (df_wms[COL_TRANS_TYPE] == '出库')].copy()
    cancel_records = df_wms[(df_wms[COL_INOUT] == 'OUT') & (df_wms[COL_TRANS_TYPE] == '取消出库')].copy()
    receive_records = df_wms[(df_wms[COL_INOUT] == 'IN') & (df_wms[COL_TRANS_TYPE] == '收货')].copy()

    # ========== 匹配函数（最新规则） ==========
    def match_outbound(row):
        lrp = clean_str(row[COL_ORDER_WMS])
        mat = clean_str(row[COL_MATERIAL_WMS])
        plant = clean_str(row[COL_PLANT_WMS])
        storage = clean_str(row[COL_STORAGE_WMS])
        common = clean_str(row[COL_COMMON_NO])

        if lrp != '':
            key = (lrp, mat, plant, storage)
            return '是' if key in outbound_order else '否'
        else:
            if common == '':
                return '否'
            if is_start_with_4(common):
                key = (common, mat, plant, storage)
                return '是' if key in outbound_po else '否'
            elif is_start_with_01(common):
                key = (common, mat, plant, storage)
                return '是' if key in outbound_order else '否'
            else:
                common_norm = normalize_number(common)
                key = (common_norm, mat, plant, storage)
                return '是' if key in outbound_reserve else '否'

    def match_inbound(row):
        lrp = clean_str(row[COL_ORDER_WMS])
        mat = clean_str(row[COL_MATERIAL_WMS])
        plant = clean_str(row[COL_PLANT_WMS])
        storage = clean_str(row[COL_STORAGE_WMS])
        common = clean_str(row[COL_COMMON_NO])

        if lrp != '':
            key = (lrp, mat, plant, storage)
            return '是' if key in inbound_order else '否'
        else:
            if common == '':
                return '否'
            if is_start_with_4(common):
                key = (common, mat, plant, storage)
                return '是' if key in inbound_po else '否'
            elif is_start_with_01(common):
                key = (common, mat, plant, storage)
                return '是' if key in inbound_order else '否'
            else:
                common_norm = normalize_number(common)
                key = (common_norm, mat, plant, storage)
                return '是' if key in inbound_reserve else '否'

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
        if record_type == 'out' and clean_str(row[COL_ORDER_WMS]) == '':
            return row[COL_QTY_WMS] / 2.0
        else:
            return row[COL_QTY_WMS]

    if not unmatched_out.empty:
        unmatched_out['数量_调整'] = unmatched_out.apply(lambda r: adjust_qty(r, 'out'), axis=1)
        unmatched_out['记录类型'] = '出库'
    if not unmatched_cancel.empty:
        unmatched_cancel['数量_调整'] = unmatched_cancel[COL_QTY_WMS]
        unmatched_cancel['记录类型'] = '取消出库'
    if not unmatched_receive.empty:
        unmatched_receive['数量_调整'] = unmatched_receive[COL_QTY_WMS]
        unmatched_receive['记录类型'] = '收货'

    # ========== 有效单号（用于展示） ==========
    def get_effective_order(row):
        lrp = clean_str(row[COL_ORDER_WMS])
        if lrp != '':
            return lrp
        else:
            common = clean_str(row[COL_COMMON_NO])
            if common != '' and not is_start_with_4(common) and not is_start_with_01(common):
                return common
            else:
                return ''

    for df_temp in [unmatched_out, unmatched_cancel, unmatched_receive]:
        if not df_temp.empty:
            df_temp['有效单号'] = df_temp.apply(get_effective_order, axis=1)
            df_temp['原始LRP'] = df_temp[COL_ORDER_WMS].apply(clean_str)

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

    # ========== 出库与取消出库抵消 ==========
    key_cols = [COL_COMMON_NO, COL_MATERIAL_WMS, COL_PLANT_WMS, COL_STORAGE_WMS]

    if not unmatched_out.empty:
        out_sum = unmatched_out.groupby(key_cols)['数量_调整'].sum().reset_index(name='出库数量')
    else:
        out_sum = pd.DataFrame(columns=key_cols + ['出库数量'])

    if not unmatched_cancel.empty:
        cancel_sum = unmatched_cancel.groupby(key_cols)['数量_调整'].sum().reset_index(name='取消出库数量')
    else:
        cancel_sum = pd.DataFrame(columns=key_cols + ['取消出库数量'])

    net_out = pd.merge(out_sum, cancel_sum, on=key_cols, how='outer').fillna(0)
    net_out['出库净数量'] = net_out['出库数量'] - net_out['取消出库数量']
    net_out = net_out[net_out['出库净数量'] > 0].copy()

    if not net_out.empty:
        out_with_keys = unmatched_out.copy()
        out_with_keys['_key'] = out_with_keys[key_cols].apply(tuple, axis=1)
        net_out['_key'] = net_out[key_cols].apply(tuple, axis=1)

        single_list = out_with_keys.groupby('_key')['有效单号'].apply(lambda x: '\n'.join(sorted(set(x)))).reset_index()
        single_list.columns = ['_key', '未匹配单号列表']
        single_list['未匹配单号个数'] = single_list['未匹配单号列表'].apply(lambda x: len(x.split('\n')) if x else 0)

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

    # ========== 收货记录 ==========
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

        receive_agg[key_cols] = pd.DataFrame(receive_agg['_key'].tolist(), index=receive_agg.index)
        receive_agg.drop(columns=['_key'], inplace=True)
        receive_agg['记录类型'] = '收货'
    else:
        receive_agg = pd.DataFrame()

    # ========== 合并汇总 ==========
    all_unmatched_agg = pd.concat([net_out, receive_agg], ignore_index=True, sort=False)

    if all_unmatched_agg.empty:
        df_summary = pd.DataFrame(columns=[COL_MATERIAL_WMS, COL_PLANT_WMS, COL_STORAGE_WMS,
                                           '未匹配单号列表', '未匹配单号个数', '数量', '返回消息', '差异类型',
                                           '出库数量', '收货数量'])
    else:
        group_cols = [COL_MATERIAL_WMS, COL_PLANT_WMS, COL_STORAGE_WMS]

        for col in ['出库净数量', '收货净数量', '未匹配单号列表', '收货单号列表', '返回消息', '差异类型']:
            if col not in all_unmatched_agg.columns:
                all_unmatched_agg[col] = '' if '列表' in col or '消息' in col or '类型' in col else 0

        final_agg = all_unmatched_agg.groupby(group_cols).agg({
            '出库净数量': lambda x: x.fillna(0).sum(),
            '收货净数量': lambda x: x.fillna(0).sum(),
            '未匹配单号列表': lambda x: '\n'.join(sorted(set('\n'.join(x.fillna('')).split('\n')))).strip('\n'),
            '收货单号列表': lambda x: '\n'.join(sorted(set('\n'.join(x.fillna('')).split('\n')))).strip('\n'),
            '返回消息': lambda x: '\n'.join(sorted(set('\n'.join(x.fillna('')).split('\n')))).strip('\n'),
            '差异类型': lambda x: ';'.join(sorted(set(';'.join(x.fillna('')).split(';')))).strip(';')
        }).reset_index()

        final_agg['未匹配单号列表'] = (final_agg['未匹配单号列表'].fillna('') + '\n' + final_agg['收货单号列表'].fillna('')).str.strip('\n')
        final_agg['未匹配单号个数'] = final_agg['未匹配单号列表'].apply(lambda x: len(x.split('\n')) if x else 0)
        final_agg['数量'] = final_agg['出库净数量'] + final_agg['收货净数量']

        df_summary = final_agg[group_cols + ['未匹配单号列表', '未匹配单号个数', '数量', '返回消息', '差异类型', '出库净数量', '收货净数量']]
        df_summary.rename(columns={'出库净数量': '出库数量', '收货净数量': '收货数量'}, inplace=True)

    # ========== 构建带标记的WMS表 ==========
    all_matched = pd.concat([
        out_records[out_records['匹配'] == '是'] if not out_records.empty else pd.DataFrame(),
        receive_records[receive_records['匹配'] == '是'] if not receive_records.empty else pd.DataFrame()
    ], ignore_index=True)

    if not all_matched.empty:
        def get_effective_order_matched(row):
            lrp = clean_str(row[COL_ORDER_WMS])
            if lrp != '':
                return lrp
            else:
                common = clean_str(row[COL_COMMON_NO])
                if common != '' and not is_start_with_4(common) and not is_start_with_01(common):
                    return common
                else:
                    return ''
        all_matched['有效单号'] = all_matched.apply(get_effective_order_matched, axis=1)
        all_matched['数量_调整'] = all_matched.apply(lambda r: adjust_qty(r, 'out') if r[COL_TRANS_TYPE]=='出库' else r[COL_QTY_WMS], axis=1)
        all_matched['记录类型'] = all_matched[COL_TRANS_TYPE]
        all_matched['返回消息'] = ''
        all_matched['差异类型'] = ''
        all_matched['原始LRP'] = all_matched[COL_ORDER_WMS].apply(clean_str)

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

        # 初始化目标报表的新列，统一为 object 类型以避免类型错误
        df_target['未匹配单号列表'] = ''
        df_target['未匹配单号个数'] = 0
        df_target['数量'] = 0
        df_target['返回消息'] = ''
        df_target['差异类型'] = ''
        df_target['调整后差异'] = 0.0
        df_target['未匹配单号列表'] = df_target['未匹配单号列表'].astype(object)
        df_target['返回消息'] = df_target['返回消息'].astype(object)
        df_target['差异类型'] = df_target['差异类型'].astype(object)

        if not df_summary.empty:
            summary_dict = {}
            for _, row in df_summary.iterrows():
                key = (row[COL_MATERIAL_WMS], row[COL_PLANT_WMS], row[COL_STORAGE_WMS])
                summary_dict[key] = {
                    '未匹配单号列表': row['未匹配单号列表'],
                    '未匹配单号个数': int(row['未匹配单号个数']),
                    '数量': int(row['数量']),
                    '返回消息': row['返回消息'],
                    '差异类型': row['差异类型'],
                    '出库数量': int(row['出库数量']),
                    '收货数量': int(row['收货数量'])
                }

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
                    net_qty = rec['数量']
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
        skip_rdc = st.checkbox("🚫 跳过 RDC 仓库匹配", value=False)
        st.markdown("---")
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

    # 映射表处理
    if st.session_state['need_po_map']:
        st.warning("⚠️ 检测到非 L 开头的前继单号，请下载模板填写对应的 LRP 单号。")
        if st.session_state['template_df'] is not None:
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as w:
                st.session_state['template_df'].to_excel(w, index=False, sheet_name='模板')
            st.download_button(
                label="📥 下载采购订单映射模板",
                data=buffer.getvalue(),
                file_name=f"采购订单映射模板_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        po_map_file = st.file_uploader("📎 上传填写好的映射表", type=['xlsx', 'xls', 'csv'], key="po_map")
        c1, c2 = st.columns([1, 3])
        with c1:
            if st.button("✅ 使用映射表对账", type="primary"):
                if po_map_file is None:
                    st.error("请先上传映射表")
                else:
                    try:
                        df_map = read_file(po_map_file)
                        df_r3_mapped = apply_po_mapping(st.session_state['pending_r3'], df_map)
                        with st.spinner("⏳ 正在处理数据，请稍候..."):
                            res = process_data(
                                st.session_state['pending_wms'],
                                df_r3_mapped,
                                st.session_state['pending_sales'],
                                st.session_state['pending_target'],
                                st.session_state['pending_rdc'],
                                st.session_state['pending_skip_rdc']
                            )
                        st.session_state['last_reconciliation_result'] = res[2]
                        st.session_state['last_summary'] = res[1]
                        st.session_state['last_wms_marked'] = res[0]
                        st.session_state['reconciliation_done'] = True
                        st.session_state['need_po_map'] = False
                        for k in ['pending_wms', 'pending_r3', 'pending_target', 'pending_sales', 'pending_rdc', 'template_df']:
                            st.session_state[k] = None
                        st.success("🎉 对账完成！")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ 处理失败: {str(e)}")
                        st.exception(e)
        with c2:
            if st.button("⏭️ 跳过映射，直接对账"):
                with st.spinner("⏳ 正在处理数据，请稍候..."):
                    res = process_data(
                        st.session_state['pending_wms'],
                        st.session_state['pending_r3'],
                        st.session_state['pending_sales'],
                        st.session_state['pending_target'],
                        st.session_state['pending_rdc'],
                        st.session_state['pending_skip_rdc']
                    )
                st.session_state['last_reconciliation_result'] = res[2]
                st.session_state['last_summary'] = res[1]
                st.session_state['last_wms_marked'] = res[0]
                st.session_state['reconciliation_done'] = True
                st.session_state['need_po_map'] = False
                for k in ['pending_wms', 'pending_r3', 'pending_target', 'pending_sales', 'pending_rdc', 'template_df']:
                    st.session_state[k] = None
                st.success("🎉 对账完成！")
                st.rerun()
        st.stop()

    # 开始对账
    if st.button("🚀 开始对账", type="primary", use_container_width=True):
        if not all([wms_file, r3_file, target_file]):
            st.error("❌ 请至少上传 WMS交易记录、R3交易记录 和 WMS与R3库存差异报表 三个文件")
        else:
            gc.collect()
            with st.spinner("⏳ 正在读取文件..."):
                try:
                    df_wms = read_file(wms_file)
                    df_r3 = read_file(r3_file)
                    df_target = read_file(target_file)
                    df_sales = read_file(sales_file) if sales_file else None
                    df_rdc = read_file(rdc_file) if rdc_file else None
                except Exception as e:
                    st.error(f"❌ 文件读取失败: {str(e)}")
                    st.stop()

            df_r3['_clean_order'] = df_r3[COL_ORDER_R3].astype(str).apply(clean_str)
            mask = (df_r3['_clean_order'] != '') & (~df_r3['_clean_order'].str.startswith('L'))
            if mask.any():
                if COL_PO_R3 in df_r3.columns:
                    sub = df_r3.loc[mask, [COL_ORDER_R3, COL_PO_R3]].copy()
                else:
                    st.error(f"R3 表中缺少 '{COL_PO_R3}' 列，无法生成模板。")
                    st.stop()
                sub[COL_ORDER_R3] = sub[COL_ORDER_R3].astype(str).apply(clean_str)
                sub[COL_PO_R3] = sub[COL_PO_R3].astype(str).apply(clean_str)
                sub = sub.drop_duplicates()
                template = pd.DataFrame({
                    COL_TEMPLATE_PO: sub[COL_PO_R3],
                    COL_TEMPLATE_OLD_ORDER: sub[COL_ORDER_R3],
                    COL_TEMPLATE_LRP: ''
                })
                st.session_state['template_df'] = template
                st.session_state['need_po_map'] = True
                st.session_state['pending_wms'] = df_wms
                st.session_state['pending_r3'] = df_r3.drop(columns=['_clean_order'])
                st.session_state['pending_target'] = df_target
                st.session_state['pending_sales'] = df_sales
                st.session_state['pending_rdc'] = df_rdc
                st.session_state['pending_skip_rdc'] = skip_rdc
                st.rerun()
            else:
                df_r3.drop(columns=['_clean_order'], inplace=True)
                with st.spinner("⏳ 正在处理数据，请稍候..."):
                    try:
                        res = process_data(df_wms, df_r3, df_sales, df_target, df_rdc, skip_rdc)
                        st.session_state['last_reconciliation_result'] = res[2]
                        st.session_state['last_summary'] = res[1]
                        st.session_state['last_wms_marked'] = res[0]
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
                with pd.ExcelWriter(buffer, engine='openpyxl') as w:
                    df_summary.to_excel(w, sheet_name='未匹配汇总', index=False)
                st.download_button(
                    label="📄 下载未匹配汇总",
                    data=buffer.getvalue(),
                    file_name=f"未匹配汇总_{timestamp}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

        with col_d2:
            if df_wms_marked is not None and not df_wms_marked.empty:
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as w:
                    df_wms_marked.to_excel(w, sheet_name='WMS交易记录_带匹配标记', index=False)
                st.download_button(
                    label="📄 下载带标记WMS表",
                    data=buffer.getvalue(),
                    file_name=f"WMS交易记录_带匹配标记_{timestamp}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

        with col_d3:
            if df_result is not None and not df_result.empty:
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as w:
                    df_result.to_excel(w, sheet_name='库存差异报表_带未匹配单号', index=False)
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
    | R3交易记录 | ✅ | 包含 前继单号、数量、预留编号、物料代码、工厂、库位、采购订单 |
    | WMS与R3库存差异报表 | ✅ | 包含 货品编号、工厂编码、库位编码、WMS和ERP的差异库存、仓库编码 |
    | 销售下单异常报表 | ❌ | 包含 运单号、返回消息 |
    | RDC仓库编号 | ❌ | 包含 仓库编号 |

    ### 🚀 操作步骤
    1. 上传必需文件，点击"开始对账"
    2. 若 R3 中存在非 L 开头的前继单号，工具会生成映射模板，请下载并填写对应的 LRP 单号后上传
    3. 查看结果并下载 Excel 报表

    ### 💡 匹配规则（最新版）
    - **LRP非空**：LRP单号 + 货品编码 + 工厂 + ERP库位 ↔ 前继单号 + 物料代码 + 工厂 + 库位
    - **LRP为空且单号不以4/01开头**：单号(去前导0) + 货品编码 + 工厂 + ERP库位 ↔ 预留编号(去前导0) + 物料代码 + 工厂 + 库位
    - **LRP为空且单号以4开头**：单号(原值) + 货品编码 + 工厂 + ERP库位 ↔ 采购订单 + 物料代码 + 工厂 + 库位
    - **LRP为空且单号以01开头**：单号(原值) + 货品编码 + 工厂 + ERP库位 ↔ 前继单号 + 物料代码 + 工厂 + 库位
    - **取消出库抵消**：按 单号+货品编码+工厂+ERP库位 四字段完全匹配，净数量>0才保留。
    - **数量调整**：仅出库且LRP为空时数量除以2。
    - **调整后差异**：正差异减净未匹配量，负差异加净未匹配量。
    - **系统API保管员**：仅保留收货且LRP空、单号不以01/4开头的记录，其余系统API记录删除。
    """)
