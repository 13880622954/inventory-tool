import streamlit as st
import pandas as pd
import io

# 设置页面标题和布局
st.set_page_config(page_title="库存核对工具", layout="wide")

# ==================== 侧边栏：文件上传 ====================
st.sidebar.title("📂 上传文件")
pisao_file = st.sidebar.file_uploader("批扫表 (Excel)", type=["xlsx", "xls"])
warehouse_file = st.sidebar.file_uploader("仓库编号表 (Excel)", type=["xlsx", "xls"])

st.sidebar.markdown("---")
st.sidebar.info("请确保列名正确：\n- 批扫表：`warehouseCode`, `erpLocationCode`, `itemCode`, `TYPE`, `qty`\n- 仓库编号表：`仓库编号`")

# ==================== 主区域 ====================
st.title("📊 库存差异核对工具")

# 数据处理函数（完全基于您之前的逻辑，只改动了输入输出为内存对象）
def process_data(pisao_bytes, warehouse_bytes):
    df_pisao = pd.read_excel(pisao_bytes)
    df_warehouse = pd.read_excel(warehouse_bytes)

    # 匹配仓库编号
    valid_warehouses = set(df_warehouse['仓库编号'].dropna().unique())
    mask_matched = df_pisao['warehouseCode'].isin(valid_warehouses)
    df_matched = df_pisao[mask_matched].copy()

    # 获取尾数
    df_matched['erp_tail'] = df_matched['erpLocationCode'].astype(str).str[-1]

    # 分类
    white_list = ['1', 'C', 'N', 'Q', 'E']
    cond_tail_6 = df_matched['erp_tail'] == '6'
    cond_other_abnormal = ~df_matched['erp_tail'].isin(white_list + ['6'])
    cond_normal = df_matched['erp_tail'].isin(white_list)

    df_tail_6 = df_matched[cond_tail_6].copy()
    df_other_abnormal = df_matched[cond_other_abnormal].copy()
    df_normal = df_matched[cond_normal].copy()

    # 删除临时列
    for df in [df_tail_6, df_other_abnormal, df_normal]:
        if 'erp_tail' in df.columns:
            df.drop(columns=['erp_tail'], inplace=True)

    # 统计正常明细
    if len(df_normal) > 0:
        grouped = df_normal.groupby(['itemCode', 'warehouseCode', 'TYPE'])['qty'].sum().reset_index()
        pivot = grouped.pivot_table(index=['itemCode', 'warehouseCode'],
                                    columns='TYPE',
                                    values='qty',
                                    fill_value=0).reset_index()
        if '出库' not in pivot.columns:
            pivot['出库'] = 0
        if '入库' not in pivot.columns:
            pivot['入库'] = 0
        pivot['差异'] = pivot['出库'] - pivot['入库']
        df_full_stats = pivot.copy()
        df_diff = pivot[pivot['差异'] != 0].copy()
        df_diff['核对备注'] = '需要核对'
    else:
        df_full_stats = pd.DataFrame(columns=['itemCode', 'warehouseCode', '出库', '入库', '差异'])
        df_diff = pd.DataFrame(columns=['itemCode', 'warehouseCode', '出库', '入库', '差异', '核对备注'])

    # 写入 Excel 到内存
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_tail_6.to_excel(writer, sheet_name='尾数为6明细', index=False)
        df_other_abnormal.to_excel(writer, sheet_name='其他异常尾数明细', index=False)
        df_normal.to_excel(writer, sheet_name='正常明细', index=False)
        df_full_stats.to_excel(writer, sheet_name='出入库统计全量', index=False)
        df_diff.to_excel(writer, sheet_name='差异核对表', index=False)
    output.seek(0)
    return output, len(df_tail_6), len(df_other_abnormal), len(df_normal), len(df_full_stats), len(df_diff)

# ==================== 当两个文件都上传后自动处理 ====================
if pisao_file and warehouse_file:
    with st.spinner("正在处理数据，请稍候..."):
        try:
            result_excel, n_tail6, n_other, n_normal, n_stats, n_diff = process_data(pisao_file, warehouse_file)
            
            # 显示统计摘要（三列布局）
            col1, col2, col3 = st.columns(3)
            col1.metric("异常：尾数为6", n_tail6)
            col2.metric("异常：其他尾数", n_other)
            col3.metric("正常明细行数", n_normal)
            
            col4, col5 = st.columns(2)
            col4.metric("统计汇总行数", n_stats)
            col5.metric("需要核对的行数", n_diff, delta="差异≠0")
            
            # 下载按钮
            st.download_button(
                label="📥 下载结果 Excel 文件",
                data=result_excel,
                file_name="处理结果.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            st.success("处理完成！点击上方按钮下载结果文件。")
            
        except Exception as e:
            st.error(f"处理出错：{str(e)}")
else:
    st.info("请在左侧边栏上传批扫表和仓库编号表（Excel 格式）。")
